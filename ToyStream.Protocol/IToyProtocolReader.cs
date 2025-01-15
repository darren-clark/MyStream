namespace ToyStream.Protocol;

public interface IToyProtocolReader
{
    Task Read(IToyProtocolHandler handler, CancellationToken cancellationToken);
}
