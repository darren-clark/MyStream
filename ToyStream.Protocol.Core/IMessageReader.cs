namespace ToyStream.Protocol;

public interface IMessageReader
{
    Task Read(IMessageHandler handler, CancellationToken cancellationToken);
}