namespace ToyStream.Protocol.Messages;

public interface IMessage
{
    public int Size { get; }
    public MessageType Type { get; }

}