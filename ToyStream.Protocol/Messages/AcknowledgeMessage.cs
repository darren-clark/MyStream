namespace ToyStream.Protocol.Messages;

using Responses;

public ref struct AcknowledgeMessage: IResponseMessage<AcknowledgeMessage, AcknowledgeResponse>
{
    public MessageType Type => MessageType.Acknowledge;
    
    static bool IMessage<AcknowledgeMessage>.TryRead<TReader>(ref TReader reader, out AcknowledgeMessage message, out int bytesToRead)
    {
        message = new AcknowledgeMessage();
        bytesToRead = 0;
        return true;
    }

    void IMessage<AcknowledgeMessage>.Write<TWriter>(TWriter writer)
    {
    }

    public AcknowledgeResponse Response => AcknowledgeResponse.Instance;
}