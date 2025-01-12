namespace ToyStream.Protocol.Send;

public interface IMessage: IProtocolComponent
{
    public MessageType Type { get; }
}