using ToyStream.Protocol;
using ToyStream.Protocol.Messages;
using ToyStream.Protocol.Responses;
using ToyStream.Shared;

namespace ToyStream.Server;

/// <summary>
/// Topic administration. A connection in this role structurally cannot publish or fetch, so those
/// message types reaching it are a protocol violation rather than an unimplemented handler.
/// </summary>
public readonly struct AdminHandler(ITopicService topicService) : IConnectionHandler
{
    private readonly CreateTopicHandler createTopic = new(topicService);
    private readonly DescribeTopicHandler describeTopic = new(topicService);

    public DispatchResult TryParseAndDispatch(MessageType messageType, ref Dispatcher dispatch) =>
        messageType switch
        {
            MessageType.CreateTopic =>
                dispatch.Request<CreateTopicHandler, CreateTopicMessage, AcknowledgeMessage<CreateTopicResult>>(
                    this.createTopic),
            MessageType.DescribeTopic =>
                dispatch.Request<DescribeTopicHandler, DescribeTopicRequestMessage, DescribeTopicResponseMessage>(
                    this.describeTopic),
            _ => dispatch.Unexpected(messageType)
        };
}
