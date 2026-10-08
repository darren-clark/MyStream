using System.Buffers;
using System.IO.Pipelines;
using ToyStream.Protocol.Messages;

namespace ToyStream.Protocol.Dispatch.Context;

public readonly struct ServerContext(ITopicService topicService):
    IConnectionContext,
    IRequestHandler<CreateTopicMessage, AcknowledgeMessage>
{
    public DispatchResult TryParseAndDispatch(ref ReadOnlySequence<byte> buffer, ref int bytesNeeded,
        PipeWriter writer, CancellationToken cancellationToken)
    {
        if (buffer.IsEmpty)
        {
            bytesNeeded = 1;
            return DispatchResult.Insufficient;
        }

        var messageType = (MessageType) buffer.FirstSpan[0];

        switch (messageType)
        {
            case MessageType.CreateTopic:
                return MessageDispatcher.TryParseAndDispatchRequest<ServerContext, CreateTopicMessage, AcknowledgeMessage>(
                    this, writer, ref buffer, ref bytesNeeded, cancellationToken);
            default:
                return MessageDispatcher.ThrowUnexpectedMessage(messageType);
        }
    }

    public ValueTask DispatchRequest(CreateTopicMessage message, ResponseWriter<AcknowledgeMessage> responseWriter,
        CancellationToken cancellationToken)
    {
        var createTopic = topicService.CreateTopic(message.TopicName, message.PartitionCount);
        responseWriter.WriteResponse(new AcknowledgeMessage());
        return createTopic;
    }
}