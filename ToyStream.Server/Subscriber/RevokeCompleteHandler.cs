using ToyStream.Protocol;
using ToyStream.Protocol.Messages;

namespace ToyStream.Server;

/// <summary>
/// Finishes a handover. One way like the revoke it answers, so the read loop still writes nothing
/// and the pump stays the connection's only writer.
/// </summary>
public readonly struct RevokeCompleteHandler(SubscriptionPump pump) : IRequestHandler<RevokeCompleteMessage>
{
    public ValueTask DispatchRequest(RevokeCompleteMessage message, CancellationToken cancellationToken)
    {
        pump.CompleteRevoke(message.Epoch, message.Offsets);
        return default;
    }
}
