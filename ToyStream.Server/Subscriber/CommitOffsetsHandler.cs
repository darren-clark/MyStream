using ToyStream.Protocol;
using ToyStream.Protocol.Messages;

namespace ToyStream.Server;

/// <summary>
/// Records a subscriber's progress and returns. One way, so the read loop still writes nothing and
/// the pump stays the connection's only writer.
/// </summary>
public readonly struct CommitOffsetsHandler(SubscriptionPump pump) : IRequestHandler<CommitOffsetsMessage>
{
    public ValueTask DispatchRequest(CommitOffsetsMessage message, CancellationToken cancellationToken)
    {
        pump.Commit(message.Offsets);
        return default;
    }
}
