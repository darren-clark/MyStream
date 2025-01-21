namespace ToyStream.Protocol;

public interface IToyProtocolWriter
{
    ValueTask Write<T>(T message, CancellationToken cancellationToken) where T : struct, IMessage<T>, allows ref struct;
}