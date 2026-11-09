namespace ToyStream.Protocol.Messages;

/// <summary>
/// Empty: the connection already says which member is leaving, and there is nothing to add. Its
/// value is entirely in being distinguishable from the connection simply ending.
/// </summary>
public readonly ref struct LeaveGroupMessage : IRequestMessage<LeaveGroupMessage>
{
    static bool IMessage<LeaveGroupMessage>.TryRead<TReader>(ref TReader reader, out LeaveGroupMessage message,
        out int bytesToRead)
    {
        message = default;
        bytesToRead = 0;
        return true;
    }

    void IMessage.Write<TWriter>(TWriter writer)
    {
    }

    public static MessageType Type => MessageType.LeaveGroup;
}
