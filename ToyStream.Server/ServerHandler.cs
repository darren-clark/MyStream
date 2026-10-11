using System.IO.Pipelines;
using ToyStream.Protocol;
using ToyStream.Protocol.Messages;

namespace ToyStream.Server;

/// <summary>
/// Server role: its switch is exactly the request set, and implementing IRequestHandler for each arm
/// is what makes a missing handler a compile error.
/// </summary>
public readonly struct ServerHandler(ITopicService topicService):
    IConnectionHandler,
    IRequestHandler<CreateTopicMessage, AcknowledgeMessage>
{
    public DispatchResult TryParseAndDispatch(MessageType messageType, ref PrimitiveReader reader,
        PrimitiveWriter writer, CancellationToken cancellationToken) =>
        messageType switch
        {
            MessageType.CreateTopic => ConnectionHandler
                .TryParseAndDispatchRequest<ServerHandler, CreateTopicMessage, AcknowledgeMessage>(this, writer,
                    ref reader, cancellationToken),
            _ => ConnectionHandler.ThrowUnexpectedMessage(messageType)
        };

    public ValueTask DispatchRequest(CreateTopicMessage message, ResponseWriter<AcknowledgeMessage> responseWriter,
        CancellationToken cancellationToken)
    {
        var createTopic = topicService.CreateTopic(message.TopicName, message.PartitionCount);
        responseWriter.WriteResponse(new AcknowledgeMessage());
        return createTopic;
    }
}