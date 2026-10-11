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
    private readonly PublishEventReceiver publishReceiver = new();

    public DispatchResult TryParseAndDispatch(MessageType messageType, ref PrimitiveReader reader,
        PrimitiveWriter writer, CancellationToken cancellationToken) =>
        messageType switch
        {
            MessageType.CreateTopic => ConnectionHandler
                .TryParseAndDispatchRequest<ServerHandler, CreateTopicMessage, AcknowledgeMessage<CreateTopicResult>>(this, writer,
                    ref reader, cancellationToken),
            MessageType.PublishEvent => this.TryParseAndStreamPublish(ref reader),
            _ => ConnectionHandler.ThrowUnexpectedMessage(messageType)
        };

    public ValueTask DispatchRequest(CreateTopicMessage message,
        ResponseWriter<AcknowledgeMessage<CreateTopicResult>> responseWriter,
        CancellationToken cancellationToken)
        => ConnectionHandler.AwaitResultAndWriteAcknowledge(
            topicService.CreateTopic(message.TopicName, message.PartitionCount), responseWriter);

    private DispatchResult TryParseAndStreamPublish(ref PrimitiveReader reader)
    {
        if (!ConnectionHandler.TryParseRequest<PublishMessage, AcknowledgeMessage<PublishResult>>(
                ref reader, out var message, out var bytesNeeded))
        {
            return DispatchResult.Insufficient(bytesNeeded);
        }

        // Resolve now: Topic points into the buffer the pump is about to advance past.
        var partition = topicService.GetPartition(message.Topic, message.Partition, out var failure);
        return DispatchResult.Streaming(message.BodyLength, publishReceiver.Begin(partition, failure));
    }
}