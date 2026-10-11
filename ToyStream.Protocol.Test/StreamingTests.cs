namespace ToyStream.Protocol.Test;

using System.Buffers;
using System.IO.Pipelines;
using ToyStream.Protocol.Messages;
using ToyStream.Protocol.Responses;
using ToyStream.Shared;

public class StreamingTests
{
    private static (Pipe ClientToServer, Pipe ServerToClient) CreatePipes() => (new Pipe(), new Pipe());

    [TestCase(4)]
    [TestCase(64 * 1024)]
    public async Task PayloadIsStreamedWholeRegardlessOfSize(int payloadSize)
    {
        var (clientToServer, serverToClient) = CreatePipes();
        using var cancellation = new CancellationTokenSource();

        var payload = new byte[payloadSize];
        Random.Shared.NextBytes(payload);

        var received = new RecordingReceiver();
        var server = new Connection<StreamingServerHandler>(
            clientToServer.Reader, serverToClient.Writer, new StreamingServerHandler(received));

        var client = new Connection<NoRequestHandler>(
            serverToClient.Reader, clientToServer.Writer, new NoRequestHandler());

        // Start returns only when the read loop ends, so it is deliberately not awaited here.
        _ = server.Start(cancellation.Token);
        _ = client.Start(cancellation.Token);

        var response = await client.SendStreamingMessage<PublishMessage, AcknowledgeMessage<PublishResult>, AcknowledgeResponse<PublishResult>>(
            new PublishMessage { Topic = new Utf8String("topic"), Partition = 2, BodyLength = payload.Length },
            new ReadOnlySequence<byte>(payload),
            cancellation.Token);

        Assert.That(response.Result, Is.EqualTo(PublishResult.Success));
        Assert.That(received.Bytes, Is.EqualTo(payload));
        await cancellation.CancelAsync();
    }

    [Test]
    public async Task StreamedMessageIsFollowedByTheNextMessage()
    {
        var (clientToServer, serverToClient) = CreatePipes();
        using var cancellation = new CancellationTokenSource();

        var received = new RecordingReceiver();
        var server = new Connection<StreamingServerHandler>(
            clientToServer.Reader, serverToClient.Writer, new StreamingServerHandler(received));

        var client = new Connection<NoRequestHandler>(
            serverToClient.Reader, clientToServer.Writer, new NoRequestHandler());

        _ = server.Start(cancellation.Token);
        _ = client.Start(cancellation.Token);

        // A desynchronised stream would mis-parse the second message rather than fail outright.
        for (var i = 0; i < 5; i++)
        {
            var payload = new byte[1024];
            Random.Shared.NextBytes(payload);

            var response = await client.SendStreamingMessage<PublishMessage, AcknowledgeMessage<PublishResult>, AcknowledgeResponse<PublishResult>>(
                new PublishMessage { Topic = new Utf8String("topic"), Partition = 2, BodyLength = payload.Length },
                new ReadOnlySequence<byte>(payload),
                cancellation.Token);

            Assert.That(response.Result, Is.EqualTo(PublishResult.Success), $"message {i}");
            Assert.That(received.Bytes, Is.EqualTo(payload), $"message {i}");
        }

        await cancellation.CancelAsync();
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

            if (!ConnectionHandler.TryParseRequest<PublishMessage, AcknowledgeMessage<PublishResult>>(
                    ref reader, out var message, out var bytesNeeded))
            {
                return DispatchResult.Insufficient(bytesNeeded);
            }

            return DispatchResult.Streaming(message.BodyLength, receiver);
        }
    }

    private readonly struct NoRequestHandler : IConnectionHandler
    {
        public DispatchResult TryParseAndDispatch(MessageType messageType, ref PrimitiveReader reader,
            PrimitiveWriter writer, CancellationToken cancellationToken) =>
            ConnectionHandler.ThrowUnexpectedMessage(messageType);
    }
}
