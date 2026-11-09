namespace ToyStream.Client;

using ToyStream.Protocol;
using ToyStream.Protocol.Messages;

/// <summary>
/// Where a revoke goes once the read loop has it. An interface rather than a delegate because the
/// handler is a struct and the subscriber outlives every message it is handed.
/// </summary>
internal interface IRevocationSink
{
    ValueTask Revoked(int epoch, short[] partitions, CancellationToken cancellationToken);
}

/// <summary>
/// Subscriber role. Records and losses arrive unbidden; only a revoke is answered, and only with
/// where this subscriber had already committed.
/// </summary>
internal readonly struct SubscriberClientHandler(SubscriptionStream stream, IRevocationSink revocations) :
    IConnectionHandler,
    IStreamingRequestHandler<PushRecordsMessage>,
    IRequestHandler<SubscriptionLostMessage>,
    IRequestHandler<RevokeMessage>
{
    public DispatchResult TryParseAndDispatch(MessageType messageType, ref Dispatcher dispatch) =>
        messageType switch
        {
            MessageType.PushRecords =>
                dispatch.StreamingRequest<SubscriberClientHandler, PushRecordsMessage>(this),
            MessageType.SubscriptionLost =>
                dispatch.Request<SubscriberClientHandler, SubscriptionLostMessage>(this),
            MessageType.Revoke =>
                dispatch.Request<SubscriberClientHandler, RevokeMessage>(this),
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

    /// <summary>
    /// Answered from here rather than from the reader: the revoke sits behind every record it covers,
    /// so by the time it is dispatched there is nothing left outstanding to wait for.
    /// </summary>
    public ValueTask DispatchRequest(RevokeMessage message, CancellationToken cancellationToken)
    {
        // Copied out of the ref struct: the answer outlives this buffer.
        var partitions = new short[message.Partitions.Count];
        for (var i = 0; i < partitions.Length; i++)
        {
            partitions[i] = message.Partitions[i];
        }

        return revocations.Revoked(message.Epoch, partitions, cancellationToken);
    }
}
