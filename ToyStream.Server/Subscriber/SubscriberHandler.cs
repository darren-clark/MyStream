using ToyStream.Protocol;
using ToyStream.Protocol.Messages;

namespace ToyStream.Server;

/// <summary>
/// A subscriber connection reads a subscribe and then only ever its progress: everything carrying
/// records travels the other way, which is what keeps the pump the sole writer.
/// </summary>
public readonly struct SubscriberHandler(SubscriptionPump pump) : IConnectionHandler
{
    private readonly SubscribeHandler subscribe = new(pump);
    private readonly CommitOffsetsHandler commit = new(pump);
    private readonly RevokeCompleteHandler revokeComplete = new(pump);
    private readonly LeaveGroupHandler leaveGroup = new(pump);

    public DispatchResult TryParseAndDispatch(MessageType messageType, ref Dispatcher dispatch) =>
        messageType switch
        {
            MessageType.Subscribe => dispatch.Request<SubscribeHandler, SubscribeMessage>(this.subscribe),
            MessageType.CommitOffsets =>
                dispatch.Request<CommitOffsetsHandler, CommitOffsetsMessage>(this.commit),
            MessageType.RevokeComplete =>
                dispatch.Request<RevokeCompleteHandler, RevokeCompleteMessage>(this.revokeComplete),
            MessageType.LeaveGroup =>
                dispatch.Request<LeaveGroupHandler, LeaveGroupMessage>(this.leaveGroup),
            _ => dispatch.Unexpected(messageType)
        };
}
