using ToyStream.Protocol;
using ToyStream.Protocol.Messages;
using ToyStream.Shared;

namespace ToyStream.Server;

/// <summary>
/// Hands the subscription to the pump and returns. One way, so nothing is written back from the
/// read loop and the pump stays the connection's only writer.
/// </summary>
public readonly struct SubscribeHandler(SubscriptionPump pump) : IRequestHandler<SubscribeMessage>
{
    public ValueTask DispatchRequest(SubscribeMessage message, CancellationToken cancellationToken)
    {
        // Copied out of the ref struct: the pump reads it long after this buffer is recycled.
        var topic = new byte[message.Topic.GetByteCount()];
        message.Topic.CopyTo(topic);

        pump.Begin(new Utf8String(topic), message.StartOffset);
        return default;
    }
}
