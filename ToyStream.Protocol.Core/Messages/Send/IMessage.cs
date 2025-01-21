namespace ToyStream.Protocol.Messages.Send;

public interface IMessage
{
    public int Size { get; }
    public MessageType Type { get; }

}