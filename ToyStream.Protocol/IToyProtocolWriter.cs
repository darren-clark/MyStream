namespace ToyStream.Protocol.Send;

public interface IToyProtocolWriter
{
    ValueTask Write<T>(T message, CancellationToken cancellationToken) where T : struct, IMessage<T>, allows ref struct;
}