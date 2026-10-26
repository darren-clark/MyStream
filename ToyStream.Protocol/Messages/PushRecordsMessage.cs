namespace ToyStream.Protocol.Messages;

/// <summary>
/// A batch of records for a subscription. The header is empty: a subscription covers one topic, and
/// each record names its own partition and offset, so there is nothing left to say up front.
/// </summary>
/// <remarks>
/// A plain struct rather than a ref struct, so the pump can build one inside its async loop.
/// </remarks>
public readonly struct PushRecordsMessage : IStreamingRequestMessage<PushRecordsMessage>
{
    static bool IMessage<PushRecordsMessage>.TryRead<TReader>(ref TReader reader, out PushRecordsMessage message,
        out int bytesToRead)
    {
        message = default;
        bytesToRead = 0;
        return true;
    }

    void IMessage.Write<TWriter>(TWriter writer)
    {
    }

    public static MessageType Type => MessageType.PushRecords;
}
