using System.IO.Pipelines;
using ToyStream.Protocol;
using ToyStream.Protocol.Messages;
using ToyStream.Protocol.Responses;

namespace ToyStream.Server;

/// <summary>
/// Server role: its switch is exactly the request set, and implementing IRequestHandler for each arm
/// is what makes a missing handler a compile error.
/// </summary>
public readonly struct ServerHandler(ITopicService topicService):
    IConnectionHandler,
    IRequestHandler<CreateTopicMessage, AcknowledgeMessage<CreateTopicResult>>
{
    public DispatchResult TryParseAndDispatch(MessageType messageType, ref PrimitiveReader reader,
        PrimitiveWriter writer, CancellationToken cancellationToken) =>
        messageType switch
        {
            MessageType.CreateTopic => ConnectionHandler
                .TryParseAndDispatchRequest<ServerHandler, CreateTopicMessage, AcknowledgeMessage<CreateTopicResult>>(this, writer,
                    ref reader, cancellationToken),
            _ => ConnectionHandler.ThrowUnexpectedMessage(messageType)
        };

    public ValueTask DispatchRequest(CreateTopicMessage message,
        ResponseWriter<AcknowledgeMessage<CreateTopicResult>> responseWriter,
        CancellationToken cancellationToken)
        => ConnectionHandler.AwaitResultAndWriteAcknowledge(
            topicService.CreateTopic(message.TopicName, message.PartitionCount), responseWriter);
}