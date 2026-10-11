namespace ToyStream.Protocol.Test;

using System.IO.Pipelines;
using ToyStream.Protocol.Messages;
using ToyStream.Protocol.Responses;
using ToyStream.Shared;

public class ConnectionTests
{
    private static (Pipe ClientToServer, Pipe ServerToClient) CreatePipes() => (new Pipe(), new Pipe());

    private static CreateTopicMessage CreateTopic(string name, short partitions) => new()
    {
        TopicName = new Utf8String(name),
        PartitionCount = partitions,
    };

    /// <summary>
    /// The send must stay pending until the peer answers - this is what the completion source cache
    /// exists for, and it is the part most easily broken by a change to the read loop.
    /// </summary>
    [Test]
    public async Task SendStaysPendingUntilResponseArrives()
    {
        var (clientToServer, serverToClient) = CreatePipes();
        using var cancellation = new CancellationTokenSource();

        var pending = new PendingRequests(clientToServer.Writer);
        var client = new Connection<ClientTestHandler>(
            serverToClient.Reader, clientToServer.Writer, new ClientTestHandler(pending));
        _ = client.Start(cancellation.Token);

        var send = pending.SendMessage<CreateTopicMessage, AcknowledgeMessage, AcknowledgeResponse>(
            CreateTopic("topic", 4), default);

        Assert.That(send.IsCompleted, Is.False, "send completed before the peer acknowledged");

        // Act as the server and acknowledge.
        var serverWriter = new PrimitiveWriter(serverToClient.Writer);
        new ResponseWriter<AcknowledgeMessage>(serverWriter).WriteResponse(new AcknowledgeMessage());
        await serverToClient.Writer.FlushAsync(cancellation.Token);

        Assert.That(await send, Is.Not.Null);
        await cancellation.CancelAsync();
    }

    [Test]
    public async Task ServerRoundTripRepeats()
    {
        var (clientToServer, serverToClient) = CreatePipes();
        using var cancellation = new CancellationTokenSource();
        var firstRequest = new TaskCompletionSource();

        var server = new Connection<ServerTestHandler>(
            clientToServer.Reader, serverToClient.Writer, new ServerTestHandler(firstRequest));

        var pending = new PendingRequests(clientToServer.Writer);
        var client = new Connection<ClientTestHandler>(
            serverToClient.Reader, clientToServer.Writer, new ClientTestHandler(pending));

        // Start returns only when the read loop ends, so it is deliberately not awaited here.
        _ = server.Start(cancellation.Token);
        _ = client.Start(cancellation.Token);

        for (var i = 0; i < 100; i++)
        {
            var response = await pending.SendMessage<CreateTopicMessage, AcknowledgeMessage, AcknowledgeResponse>(
                CreateTopic("test", 1), default);

            Assert.That(response, Is.Not.Null, $"round trip {i} returned no response");
        }

        await firstRequest.Task;
        await cancellation.CancelAsync();
    }

    private readonly struct ServerTestHandler(TaskCompletionSource received)
        : IConnectionHandler, IRequestHandler<CreateTopicMessage, AcknowledgeMessage>
    {
        public DispatchResult TryParseAndDispatch(MessageType messageType, ref PrimitiveReader reader,
            PrimitiveWriter writer, CancellationToken cancellationToken) =>
            messageType switch
            {
                MessageType.CreateTopic => ConnectionHandler
                    .TryParseAndDispatchRequest<ServerTestHandler, CreateTopicMessage, AcknowledgeMessage>(
                        this, writer, ref reader, cancellationToken),
                _ => ConnectionHandler.ThrowUnexpectedMessage(messageType)
            };

        public ValueTask DispatchRequest(CreateTopicMessage message,
            ResponseWriter<AcknowledgeMessage> responseWriter, CancellationToken cancellationToken)
        {
            received.TrySetResult();
            responseWriter.WriteResponse(new AcknowledgeMessage());
            return default;
        }
    }

    private readonly struct ClientTestHandler(PendingRequests pending) : IConnectionHandler
    {
        public DispatchResult TryParseAndDispatch(MessageType messageType, ref PrimitiveReader reader,
            PrimitiveWriter writer, CancellationToken cancellationToken) =>
            messageType switch
            {
                MessageType.Acknowledge => ConnectionHandler
                    .TryParseAndCompleteResponse<AcknowledgeMessage, AcknowledgeResponse>(pending, ref reader),
                _ => ConnectionHandler.ThrowUnexpectedMessage(messageType)
            };
    }
}
