namespace ToyStream.Protocol.Messages;

public ref struct AcknowledgeMessage: IResponseMessage<AcknowledgeMessage>
{
    public static MessageType Type => MessageType.Acknowledge;
    
    static bool IMessage<AcknowledgeMessage>.TryRead<TReader>(ref TReader reader, out AcknowledgeMessage message, out int bytesToRead)
    {
        message = new AcknowledgeMessage();
        bytesToRead = 0;
        return true;
    }

    void IMessage.Write<TWriter>(TWriter writer)
    {
    }
}