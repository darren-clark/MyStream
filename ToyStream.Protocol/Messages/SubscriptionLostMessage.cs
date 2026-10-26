namespace ToyStream.Protocol.Messages;

public enum SubscriptionLostReason : byte
{
    UnknownTopic = 1,

    /// <summary>The next record a subscriber wanted has already been evicted.</summary>
    OffsetEvicted = 2,

    ServerStopping = 3
}

/// <summary>
/// Ends a subscription the server can no longer serve. One way like everything else going out on a
/// subscriber connection; the client re-negotiates by reconnecting.
/// </summary>
public readonly struct SubscriptionLostMessage : IRequestMessage<SubscriptionLostMessage>
{
    public required SubscriptionLostReason Reason { get; init; }

    static bool IMessage<SubscriptionLostMessage>.TryRead<TReader>(ref TReader reader,
        out SubscriptionLostMessage message, out int bytesToRead)
    {
        var tryReader = reader;
        if (!tryReader.TryReadEnum<SubscriptionLostReason>(out var reason, out bytesToRead))
        {
            message = default;
            return false;
        }

        reader = tryReader;
        message = new SubscriptionLostMessage { Reason = reason };
        return true;
    }

    void IMessage.Write<TWriter>(TWriter writer) => writer.Write(this.Reason);

    public static MessageType Type => MessageType.SubscriptionLost;
}
