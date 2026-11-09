using ToyStream.Protocol;
using ToyStream.Protocol.Messages;

namespace ToyStream.Server;

/// <summary>
/// Tells the group this subscriber is going on purpose, so its partitions move now rather than
/// waiting out the linger that covers a connection simply dropping.
/// </summary>
public readonly struct LeaveGroupHandler(SubscriptionPump pump) : IRequestHandler<LeaveGroupMessage>
{
    public ValueTask DispatchRequest(LeaveGroupMessage message, CancellationToken cancellationToken)
    {
        pump.LeaveGroup();
        return default;
    }
}
