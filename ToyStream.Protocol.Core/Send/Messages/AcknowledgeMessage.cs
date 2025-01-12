namespace ToyStream.Protocol.Send;

using ToyStream.Shared;

public ref struct AcknowledgeMessage: IMessage
{
    public MessageType Type => MessageType.Acknowledge;
    public int Size => 0;
    public void Write(ref SpanWriter writer) { }
}