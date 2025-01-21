namespace ToyStream.Protocol;

public interface IMessageWriter
{
    ValueTask Write<T>(T message, CancellationToken cancellationToken) where T : struct, Messages.Send.IMessage, allows ref struct;
}