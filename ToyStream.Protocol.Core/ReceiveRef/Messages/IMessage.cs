namespace ToyStream.Protocol.ReceiveRef;

public interface IMessage<T>: IProtocolComponent<T> where T: struct, IMessage<T>, allows ref struct
{
}
