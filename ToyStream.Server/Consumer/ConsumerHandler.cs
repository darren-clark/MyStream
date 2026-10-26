using ToyStream.Protocol;
using ToyStream.Protocol.Messages;
using ToyStream.Shared;

namespace ToyStream.Server;

/// <summary>
/// Client-pull consuming: describe to resolve partitions, then fetch. Duplex and able to park for a
/// long poll, which is why it is not the same role as a pushed subscription.
/// </summary>
public readonly struct ConsumerHandler(ITopicService topicService) : IConnectionHandler
{
    private readonly DescribeTopicHandler describeTopic = new(topicService);
    private readonly FetchHandler fetch = new(topicService);

    public DispatchResult TryParseAndDispatch(MessageType messageType, ref Dispatcher dispatch) =>
        messageType switch
        {
            MessageType.DescribeTopic =>
                dispatch.Request<DescribeTopicHandler, DescribeTopicRequestMessage, DescribeTopicResponseMessage>(
                    this.describeTopic),
            MessageType.Fetch =>
                dispatch.Request<FetchHandler, FetchRequestMessage, FetchResponseMessage>(this.fetch),
            _ => dispatch.Unexpected(messageType)
        };
}
