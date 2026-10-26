using ToyStream.Protocol;
using ToyStream.Protocol.Messages;

namespace ToyStream.Server;

/// <summary>
/// A subscriber connection reads one message and then never again: everything after the subscribe
/// travels the other way, which is what keeps this half one directional.
/// </summary>
public readonly struct SubscriberHandler(SubscriptionPump pump) : IConnectionHandler
{
    private readonly SubscribeHandler subscribe = new(pump);

    public DispatchResult TryParseAndDispatch(MessageType messageType, ref Dispatcher dispatch) =>
        messageType switch
        {
            MessageType.Subscribe => dispatch.Request<SubscribeHandler, SubscribeMessage>(this.subscribe),
            _ => dispatch.Unexpected(messageType)
        };
}
