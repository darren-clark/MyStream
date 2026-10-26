namespace ToyStream.Client;

using ToyStream.Protocol;
using ToyStream.Protocol.Messages;

/// <summary>
/// Subscriber role. Everything it serves arrives unbidden and is answered with nothing, which is
/// what keeps this half of the connection one directional.
/// </summary>
internal readonly struct SubscriberClientHandler(SubscriptionStream stream) :
    IConnectionHandler,
    IStreamingRequestHandler<PushRecordsMessage>,
    IRequestHandler<SubscriptionLostMessage>
{
    public DispatchResult TryParseAndDispatch(MessageType messageType, ref Dispatcher dispatch) =>
        messageType switch
        {
            MessageType.PushRecords =>
                dispatch.StreamingRequest<SubscriberClientHandler, PushRecordsMessage>(this),
            MessageType.SubscriptionLost =>
                dispatch.Request<SubscriberClientHandler, SubscriptionLostMessage>(this),
            _ => dispatch.Unexpected(messageType)
        };

    public ValueTask DispatchRequest(StreamingMessage<PushRecordsMessage> message,
        CancellationToken cancellationToken) =>
        stream.Deliver(message.Body, cancellationToken);

    public ValueTask DispatchRequest(SubscriptionLostMessage message, CancellationToken cancellationToken)
    {
        stream.Lose(message.Reason);
        return default;
    }
}
