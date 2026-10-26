using ToyStream.Protocol;
using ToyStream.Protocol.Messages;
using ToyStream.Protocol.Responses;
using ToyStream.Shared;

namespace ToyStream.Server;

/// <summary>
/// Publishing only. A connection in this role cannot create a topic, which is the point of giving it
/// its own switch.
/// </summary>
public readonly struct ProducerHandler(ITopicService topicService) : IConnectionHandler
{
    private readonly PublishHandler publish = new(topicService);

    public DispatchResult TryParseAndDispatch(MessageType messageType, ref Dispatcher dispatch) =>
        messageType switch
        {
            MessageType.PublishEvent =>
                dispatch.StreamingRequest<PublishHandler, PublishMessage, AcknowledgeMessage<PublishResult>>(
                    this.publish),
            _ => dispatch.Unexpected(messageType)
        };
}
