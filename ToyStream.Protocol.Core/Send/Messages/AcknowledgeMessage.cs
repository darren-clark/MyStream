namespace ToyStream.Protocol.Send;

using Shared;

public ref struct AcknowledgeMessage: IMessage
{
    public MessageType Type => MessageType.Acknowledge;
    public int Size => 0;
    public void Write(ref BigEndianSpanWriter writer) { }
}