namespace ToyStream.Protocol.Test;

using System.Buffers;
using System.IO.Pipelines;
using ToyStream.Protocol.Messages;
using ToyStream.Protocol.Responses;
using ToyStream.Shared;

public class StreamingTests
{
    private static (Pipe ClientToServer, Pipe ServerToClient) CreatePipes() => (new Pipe(), new Pipe());

    /// <summary>A body already in memory goes out as a single chunk.</summary>
    [TestCase(4)]
    [TestCase(64 * 1024)]
    public async Task KnownSizeBodyRoundTrips(int bodySize)
    {
        var body = RandomBytes(bodySize);
        var received = await Publish(body, size: bodySize);

        Assert.That(received.Bytes, Is.EqualTo(body));
    }

    /// <summary>Without a size the body is framed as it arrives, so the sender need not know it.</summary>
    [TestCase(4)]
    [TestCase(64 * 1024)]
    public async Task UnknownSizeBodyRoundTrips(int bodySize)
    {
        var body = RandomBytes(bodySize);
        var received = await Publish(body, size: null);

        Assert.That(received.Bytes, Is.EqualTo(body));
    }

    /// <summary>The header carries the size so a receiver can size its buffer before the body arrives.</summary>
    [Test]
    public async Task DeclaredSizeReachesTheReceiver()
    {
        Assert.That((await Publish(RandomBytes(1024), size: 1024)).DeclaredSize, Is.EqualTo(1024L));
        Assert.That((await Publish(RandomBytes(1024), size: null)).DeclaredSize, Is.Null);
    }

    [Test]
    public async Task EmptyBodyRoundTrips()
    {
        Assert.That((await Publish([], size: 0)).Bytes, Is.Empty);
        Assert.That((await Publish([], size: null)).Bytes, Is.Empty);
    }

    /// <summary>A body larger than one chunk must be split and reassembled.</summary>
    [Test]
    public async Task BodyLongerThanAChunkIsSplit()
    {
        var body = RandomBytes(ProtocolConstants.MaxChunkSize + 1024);
        var received = await Publish(body, size: null);

        Assert.That(received.Bytes, Is.EqualTo(body));
    }

    /// <summary>A desynchronised stream would mis-parse the following message rather than fail outright.</summary>
    [Test]
    public async Task StreamedMessageIsFollowedByTheNextMessage()
    {
        var (clientToServer, serverToClient) = CreatePipes();
        using var cancellation = new CancellationTokenSource();

        var recorder = new BodyRecorder();
        var server = new Connection<StreamingServerHandler>(
            clientToServer.Reader, serverToClient.Writer, new StreamingServerHandler(recorder));
        var client = new Connection<NoRequestHandler>(
            serverToClient.Reader, clientToServer.Writer, new NoRequestHandler());

        // Start returns only when the read loop ends, so it is deliberately not awaited here.
        var serverLoop = server.Start(cancellation.Token);
        _ = client.Start(cancellation.Token);

        for (var i = 0; i < 5; i++)
        {
            var body = RandomBytes(1024);
            var response = await WithServer(
                SendPublish(client, body, i % 2 == 0 ? body.Length : null, cancellation.Token), serverLoop);

            Assert.That(response.Result, Is.EqualTo(PublishResult.Success), $"message {i}");
            Assert.That(recorder.Bytes, Is.EqualTo(body), $"message {i}");
        }

        await cancellation.CancelAsync();
    }

    /// <summary>
    /// A handler is free to ignore its body - an unknown topic does exactly that - so the connection
    /// has to consume what was left, or the next message is parsed from the middle of this one.
    /// </summary>
    [TestCase(2048)]
    [TestCase(null)]
    public async Task BodyTheHandlerNeverReadsIsDrained(int? declaredSize)
    {
        var (clientToServer, serverToClient) = CreatePipes();
        using var cancellation = new CancellationTokenSource();

        var recorder = new BodyRecorder { SkipBody = true };
        var server = new Connection<StreamingServerHandler>(
            clientToServer.Reader, serverToClient.Writer, new StreamingServerHandler(recorder));
        var client = new Connection<NoRequestHandler>(
            serverToClient.Reader, clientToServer.Writer, new NoRequestHandler());

        var serverLoop = server.Start(cancellation.Token);
        _ = client.Start(cancellation.Token);

        var ignored = await WithServer(
            SendPublish(client, RandomBytes(2048), declaredSize, cancellation.Token), serverLoop);
        Assert.That(ignored.Result, Is.EqualTo(PublishResult.Success));
        Assert.That(recorder.Bytes, Is.Empty);

        // Only parses correctly if the ignored body was consumed in full.
        recorder.SkipBody = false;
        var next = RandomBytes(512);
        var response = await WithServer(SendPublish(client, next, next.Length, cancellation.Token), serverLoop);

        Assert.That(response.Result, Is.EqualTo(PublishResult.Success));
        Assert.That(recorder.Bytes, Is.EqualTo(next));

        await cancellation.CancelAsync();
    }

    private static byte[] RandomBytes(int count)
    {
        var bytes = new byte[count];
        Random.Shared.NextBytes(bytes);
        return bytes;
    }

    private static async Task<BodyRecorder> Publish(byte[] body, long? size)
    {
        var (clientToServer, serverToClient) = CreatePipes();
        using var cancellation = new CancellationTokenSource();

        var recorder = new BodyRecorder();
        var server = new Connection<StreamingServerHandler>(
            clientToServer.Reader, serverToClient.Writer, new StreamingServerHandler(recorder));
        var client = new Connection<NoRequestHandler>(
            serverToClient.Reader, clientToServer.Writer, new NoRequestHandler());

        var serverLoop = server.Start(cancellation.Token);
        _ = client.Start(cancellation.Token);

        var response = await WithServer(SendPublish(client, body, size, cancellation.Token), serverLoop);
        Assert.That(response.Result, Is.EqualTo(PublishResult.Success));

        await cancellation.CancelAsync();
        return recorder;
    }

    /// <summary>
    /// The read loop is not awaited, so a fault in it would otherwise surface as the response never
    /// arriving rather than as the exception that caused it.
    /// </summary>
    private static async Task<T> WithServer<T>(ValueTask<T> response, Task serverLoop)
    {
        var responseTask = response.AsTask();
        if (await Task.WhenAny(responseTask, serverLoop) == serverLoop)
        {
            await serverLoop;
        }

        return await responseTask;
    }

    private static ValueTask<AcknowledgeResponse<PublishResult>> SendPublish(Connection connection, byte[] body,
        long? size, CancellationToken cancellationToken)
    {
        var bodyPipe = new Pipe();
        bodyPipe.Writer.Write(body);
        bodyPipe.Writer.Complete();

        return connection.SendStreamingMessage<PublishMessage, AcknowledgeMessage<PublishResult>, AcknowledgeResponse<PublishResult>>(
            new StreamingMessage<PublishMessage>
            {
                Header = new PublishMessage
                {
                    Topic = new Utf8String("topic"),
                    Partition = 2,
                },
                Body = bodyPipe.Reader,
                Size = size,
            },
            cancellationToken);
    }

    private sealed class BodyRecorder
    {
        private readonly ArrayBufferWriter<byte> buffer = new();

        public byte[] Bytes { get; private set; } = [];

        public long? DeclaredSize { get; set; }

        /// <summary>Leaves the body for the connection to drain, as an unknown topic would.</summary>
        public bool SkipBody { get; set; }

        public void Clear() => this.Bytes = [];

        public async ValueTask ReadAllAsync(PipeReader body, CancellationToken cancellationToken)
        {
            while (true)
            {
                var result = await body.ReadAsync(cancellationToken);
                if (result.Buffer.IsEmpty)
                {
                    break;
                }

                foreach (var segment in result.Buffer)
                {
                    this.buffer.Write(segment.Span);
                }

                body.AdvanceTo(result.Buffer.End);
            }

            this.Bytes = this.buffer.WrittenSpan.ToArray();
            this.buffer.Clear();
        }
    }

    private readonly struct StreamingServerHandler(BodyRecorder recorder)
        : IConnectionHandler, IStreamingRequestHandler<PublishMessage, AcknowledgeMessage<PublishResult>>
    {
        public DispatchResult TryParseAndDispatch(MessageType messageType, ref Dispatcher dispatch)
        {
            if (messageType != MessageType.PublishEvent)
            {
                return dispatch.Unexpected(messageType);
            }

            return dispatch
                .StreamingRequest<StreamingServerHandler, PublishMessage, AcknowledgeMessage<PublishResult>>(this);
        }

        public ValueTask DispatchRequest(StreamingMessage<PublishMessage> message,
            ResponseWriter<AcknowledgeMessage<PublishResult>> responseWriter,
            CancellationToken cancellationToken)
        {
            recorder.DeclaredSize = message.Size;
            if (recorder.SkipBody)
            {
                recorder.Clear();
                responseWriter.WriteResponse(AcknowledgeMessage.WithResult(PublishResult.Success));
                return default;
            }

            return ReadAndAcknowledge(recorder, message.Body, responseWriter, cancellationToken);
        }

        private static async ValueTask ReadAndAcknowledge(BodyRecorder recorder, PipeReader body,
            ResponseWriter<AcknowledgeMessage<PublishResult>> responseWriter,
            CancellationToken cancellationToken)
        {
            await recorder.ReadAllAsync(body, cancellationToken);
            responseWriter.WriteResponse(AcknowledgeMessage.WithResult(PublishResult.Success));
        }
    }

    private readonly struct NoRequestHandler : IConnectionHandler
    {
        public DispatchResult TryParseAndDispatch(MessageType messageType, ref Dispatcher dispatch) =>
            dispatch.Unexpected(messageType);
    }
}
