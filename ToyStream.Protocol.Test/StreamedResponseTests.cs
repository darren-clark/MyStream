namespace ToyStream.Protocol.Test;

using System.Buffers;
using System.IO.Pipelines;
using ToyStream.Protocol.Messages;

/// <summary>
/// A streamed response hands the body to whoever is awaiting, and the connection must not touch the
/// buffer again until that caller says it is finished. These drive a real connection pair.
/// </summary>
public class StreamedResponseTests
{
    private static (Pipe ClientToServer, Pipe ServerToClient) CreatePipes() => (new Pipe(), new Pipe());

    [TestCase(1)]
    [TestCase(4096)]
    [TestCase(512 * 1024)]
    public async Task BodyRoundTrips(int size)
    {
        var (clientToServer, serverToClient) = CreatePipes();
        using var cancellation = new CancellationTokenSource();

        var server = new Connection<StreamingServerHandler>(
            clientToServer.Reader, serverToClient.Writer, new StreamingServerHandler());
        var client = new Connection<NoRequestHandler>(
            serverToClient.Reader, clientToServer.Writer, new NoRequestHandler());

        _ = server.Start(cancellation.Token);
        _ = client.Start(cancellation.Token);

        var response = await client
            .SendMessageForStreamedResponse<StreamedRequestMessage, StreamedResponseMessage, StreamedResponse>(
                new StreamedRequestMessage { Size = size }, cancellation.Token)
            .AsTask().WaitAsync(TimeSpan.FromSeconds(30));

        Assert.That(response.Size, Is.EqualTo(size));
        Assert.That(await Drain(response), Is.EqualTo(Payload(size)));

        await cancellation.CancelAsync();
    }

    /// <summary>
    /// The connection is only released when the caller finishes, so a second request has to work
    /// afterwards: the buffer must have been handed back intact.
    /// </summary>
    [Test]
    public async Task ConnectionContinuesAfterAStreamedResponse()
    {
        var (clientToServer, serverToClient) = CreatePipes();
        using var cancellation = new CancellationTokenSource();

        var server = new Connection<StreamingServerHandler>(
            clientToServer.Reader, serverToClient.Writer, new StreamingServerHandler());
        var client = new Connection<NoRequestHandler>(
            serverToClient.Reader, clientToServer.Writer, new NoRequestHandler());

        _ = server.Start(cancellation.Token);
        _ = client.Start(cancellation.Token);

        for (var i = 1; i <= 3; i++)
        {
            var size = i * 1000;
            var response = await client
                .SendMessageForStreamedResponse<StreamedRequestMessage, StreamedResponseMessage, StreamedResponse>(
                    new StreamedRequestMessage { Size = size }, cancellation.Token)
                .AsTask().WaitAsync(TimeSpan.FromSeconds(30));

            Assert.That(await Drain(response), Is.EqualTo(Payload(size)), $"round trip {i} differed");
        }

        await cancellation.CancelAsync();
    }

    /// <summary>A caller that reads nothing still releases the connection, and the rest is drained.</summary>
    [Test]
    public async Task AbandonedBodyStillReleasesTheConnection()
    {
        var (clientToServer, serverToClient) = CreatePipes();
        using var cancellation = new CancellationTokenSource();

        var server = new Connection<StreamingServerHandler>(
            clientToServer.Reader, serverToClient.Writer, new StreamingServerHandler());
        var client = new Connection<NoRequestHandler>(
            serverToClient.Reader, clientToServer.Writer, new NoRequestHandler());

        _ = server.Start(cancellation.Token);
        _ = client.Start(cancellation.Token);

        var abandoned = await client
            .SendMessageForStreamedResponse<StreamedRequestMessage, StreamedResponseMessage, StreamedResponse>(
                new StreamedRequestMessage { Size = 8192 }, cancellation.Token)
            .AsTask().WaitAsync(TimeSpan.FromSeconds(30));

        abandoned.Finish();

        var next = await client
            .SendMessageForStreamedResponse<StreamedRequestMessage, StreamedResponseMessage, StreamedResponse>(
                new StreamedRequestMessage { Size = 64 }, cancellation.Token)
            .AsTask().WaitAsync(TimeSpan.FromSeconds(30));

        Assert.That(await Drain(next), Is.EqualTo(Payload(64)));
        await cancellation.CancelAsync();
    }

    private static async Task<byte[]> Drain(StreamedResponse response)
    {
        var received = new ArrayBufferWriter<byte>();
        while (true)
        {
            var result = await response.Body.ReadAsync();
            if (result.Buffer.IsEmpty)
            {
                break;
            }

            foreach (var segment in result.Buffer)
            {
                received.Write(segment.Span);
            }

            response.Body.AdvanceTo(result.Buffer.End);
        }

        response.Finish();
        return received.WrittenSpan.ToArray();
    }

    private static byte[] Payload(int size)
    {
        var payload = new byte[size];
        for (var i = 0; i < size; i++)
        {
            payload[i] = (byte)(i % 251);
        }

        return payload;
    }

    private sealed class StreamedResponse : IStreamingResponse<StreamedResponseMessage, StreamedResponse>
    {
        private readonly TaskCompletionSource drained = new(TaskCreationOptions.RunContinuationsAsynchronously);

        private StreamedResponse(int size, PipeReader body)
        {
            Size = size;
            Body = body;
        }

        public int Size { get; }

        public PipeReader Body { get; }

        public ValueTask BodyDrained => new(this.drained.Task);

        static StreamedResponse IStreamingResponse<StreamedResponseMessage, StreamedResponse>.CreateResponse(
            ref readonly StreamedResponseMessage header, PipeReader body) => new(header.Size, body);

        public void Finish() => this.drained.TrySetResult();
    }

    private readonly struct StreamedRequestMessage
        : IRequestMessage<StreamedRequestMessage, StreamedResponseMessage>
    {
        public required int Size { get; init; }

        public static MessageType Type => MessageType.Fetch;

        static bool IMessage<StreamedRequestMessage>.TryRead<TReader>(ref TReader reader,
            out StreamedRequestMessage message, out int bytesToRead)
        {
            var tryReader = reader;
            if (!tryReader.TryReadInt32(out var size, out bytesToRead))
            {
                message = default;
                return false;
            }

            reader = tryReader;
            message = new StreamedRequestMessage { Size = size };
            return true;
        }

        void IMessage.Write<TWriter>(TWriter writer) => writer.Write(Size);
    }

    private readonly struct StreamedResponseMessage : IResponseMessage<StreamedResponseMessage>
    {
        public required int Size { get; init; }

        public static ResponseType Type => ResponseType.Fetch;

        static bool IMessage<StreamedResponseMessage>.TryRead<TReader>(ref TReader reader,
            out StreamedResponseMessage message, out int bytesToRead)
        {
            var tryReader = reader;
            if (!tryReader.TryReadInt32(out var size, out bytesToRead))
            {
                message = default;
                return false;
            }

            reader = tryReader;
            message = new StreamedResponseMessage { Size = size };
            return true;
        }

        void IMessage.Write<TWriter>(TWriter writer) => writer.Write(Size);
    }

    private readonly struct StreamingServerHandler
        : IConnectionHandler, IRequestHandler<StreamedRequestMessage, StreamedResponseMessage>
    {
        public DispatchResult TryParseAndDispatch(MessageType messageType, ref Dispatcher dispatch) =>
            messageType == MessageType.Fetch
                ? dispatch.Request<StreamingServerHandler, StreamedRequestMessage, StreamedResponseMessage>(this)
                : dispatch.Unexpected(messageType);

        public ValueTask DispatchRequest(StreamedRequestMessage message,
            ResponseWriter<StreamedResponseMessage> responseWriter, CancellationToken cancellationToken)
        {
            responseWriter.WriteResponse(new StreamedResponseMessage { Size = message.Size });
            return WriteBody(responseWriter.BeginBody(), message.Size, cancellationToken);

            static async ValueTask WriteBody(MessageBodyWriter body, int size, CancellationToken cancellationToken)
            {
                try
                {
                    var payload = Payload(size);

                    // Flushed in pieces, which is what paces a reader that cannot keep up.
                    for (var offset = 0; offset < size; offset += 4096)
                    {
                        body.Write(payload.AsSpan(offset, Math.Min(4096, size - offset)));
                        await body.FlushAsync(cancellationToken).ConfigureAwait(false);
                    }

                    await body.CompleteBodyAsync(cancellationToken).ConfigureAwait(false);
                }
                finally
                {
                    body.Complete();
                }
            }
        }
    }

    private readonly struct NoRequestHandler : IConnectionHandler
    {
        public DispatchResult TryParseAndDispatch(MessageType messageType, ref Dispatcher dispatch) =>
            dispatch.Unexpected(messageType);
    }
}
