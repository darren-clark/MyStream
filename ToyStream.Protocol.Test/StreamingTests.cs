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

        Assert.That(received, Is.EqualTo(body));
    }

    /// <summary>Without a size the body is framed as it arrives, so the sender need not know it.</summary>
    [TestCase(4)]
    [TestCase(64 * 1024)]
    public async Task UnknownSizeBodyRoundTrips(int bodySize)
    {
        var body = RandomBytes(bodySize);
        var received = await Publish(body, size: null);

        Assert.That(received, Is.EqualTo(body));
    }

    [Test]
    public async Task EmptyBodyRoundTrips()
    {
        Assert.That(await Publish([], size: 0), Is.Empty);
        Assert.That(await Publish([], size: null), Is.Empty);
    }

    /// <summary>A body larger than one chunk must be split and reassembled.</summary>
    [Test]
    public async Task BodyLongerThanAChunkIsSplit()
    {
        var body = RandomBytes(ProtocolConstants.MaxChunkSize + 1024);
        var received = await Publish(body, size: null);

        Assert.That(received, Is.EqualTo(body));
    }

    /// <summary>A desynchronised stream would mis-parse the following message rather than fail outright.</summary>
    [Test]
    public async Task StreamedMessageIsFollowedByTheNextMessage()
    {
        var (clientToServer, serverToClient) = CreatePipes();
        using var cancellation = new CancellationTokenSource();

        var receiver = new RecordingReceiver();
        var server = new Connection<StreamingServerHandler>(
            clientToServer.Reader, serverToClient.Writer, new StreamingServerHandler(receiver));
        var client = new Connection<NoRequestHandler>(
            serverToClient.Reader, clientToServer.Writer, new NoRequestHandler());

        // Start returns only when the read loop ends, so it is deliberately not awaited here.
        _ = server.Start(cancellation.Token);
        _ = client.Start(cancellation.Token);

        for (var i = 0; i < 5; i++)
        {
            var body = RandomBytes(1024);
            var response = await SendPublish(client, body, i % 2 == 0 ? body.Length : null, cancellation.Token);

            Assert.That(response.Result, Is.EqualTo(PublishResult.Success), $"message {i}");
            Assert.That(receiver.Bytes, Is.EqualTo(body), $"message {i}");
        }

        await cancellation.CancelAsync();
    }

    private static byte[] RandomBytes(int count)
    {
        var bytes = new byte[count];
        Random.Shared.NextBytes(bytes);
        return bytes;
    }

    private static async Task<byte[]> Publish(byte[] body, int? size)
    {
        var (clientToServer, serverToClient) = CreatePipes();
        using var cancellation = new CancellationTokenSource();

        var receiver = new RecordingReceiver();
        var server = new Connection<StreamingServerHandler>(
            clientToServer.Reader, serverToClient.Writer, new StreamingServerHandler(receiver));
        var client = new Connection<NoRequestHandler>(
            serverToClient.Reader, clientToServer.Writer, new NoRequestHandler());

        _ = server.Start(cancellation.Token);
        _ = client.Start(cancellation.Token);

        var response = await SendPublish(client, body, size, cancellation.Token);
        Assert.That(response.Result, Is.EqualTo(PublishResult.Success));

        await cancellation.CancelAsync();
        return receiver.Bytes;
    }

    private static ValueTask<AcknowledgeResponse<PublishResult>> SendPublish(Connection connection, byte[] body,
        int? size, CancellationToken cancellationToken)
    {
        var bodyPipe = new Pipe();
        bodyPipe.Writer.Write(body);
        bodyPipe.Writer.Complete();

        return connection.SendStreamingMessage<PublishMessage, AcknowledgeMessage<PublishResult>, AcknowledgeResponse<PublishResult>>(
            new PublishMessage
            {
                Topic = new Utf8String("topic"),
                Partition = 2,
                Body = bodyPipe.Reader,
                Size = size,
            },
            cancellationToken);
    }

    private sealed class RecordingReceiver : IPayloadReceiver
    {
        private readonly ArrayBufferWriter<byte> buffer = new();

        public byte[] Bytes { get; private set; } = [];

        public ValueTask ReceiveAsync(ReadOnlySequence<byte> chunk, CancellationToken cancellationToken)
        {
            foreach (var segment in chunk)
            {
                this.buffer.Write(segment.Span);
            }

            return default;
        }

        public ValueTask CompleteAsync(PrimitiveWriter writer, CancellationToken cancellationToken)
        {
            this.Bytes = this.buffer.WrittenSpan.ToArray();
            this.buffer.Clear();

            new ResponseWriter<AcknowledgeMessage<PublishResult>>(writer)
                .WriteResponse(AcknowledgeMessage.WithResult(PublishResult.Success));
            return default;
        }
    }

    private readonly struct StreamingServerHandler(RecordingReceiver receiver) : IConnectionHandler
    {
        public DispatchResult TryParseAndDispatch(MessageType messageType, ref PrimitiveReader reader,
            PrimitiveWriter writer, CancellationToken cancellationToken)
        {
            if (messageType != MessageType.PublishEvent)
            {
                return ConnectionHandler.ThrowUnexpectedMessage(messageType);
            }

            return ConnectionHandler.TryParseRequest<PublishMessage>(ref reader, out _, out var bytesNeeded)
                ? DispatchResult.Streaming(receiver)
                : DispatchResult.Insufficient(bytesNeeded);
        }
    }

    private readonly struct NoRequestHandler : IConnectionHandler
    {
        public DispatchResult TryParseAndDispatch(MessageType messageType, ref PrimitiveReader reader,
            PrimitiveWriter writer, CancellationToken cancellationToken) =>
            ConnectionHandler.ThrowUnexpectedMessage(messageType);
    }
}
