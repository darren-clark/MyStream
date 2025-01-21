namespace ToyStream.Protocol;

using Messages.Recieve;

public interface IMessageHandler
{
    ValueTask HandleMessage<T>(IMessage<T> message, CancellationToken cancellationToken) where T : struct, IMessage<T>, allows ref struct;
}