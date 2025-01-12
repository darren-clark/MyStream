namespace ToyStream.Protocol.Receive;

public interface IToyProtocolHandler
{
    public ValueTask HandleMessage<T>(ref readonly T message) where T : struct, IMessage;
}