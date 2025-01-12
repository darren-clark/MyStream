namespace ToyStream.Protocol.ReceiveRef;

public interface IToyProtocolHandler
{
    public ValueTask HandleMessage<T>(ref readonly T message) where T : struct, IMessage<T>, allows ref struct;
}