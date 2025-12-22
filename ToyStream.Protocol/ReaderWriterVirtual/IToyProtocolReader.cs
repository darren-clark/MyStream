namespace ToyStream.Protocol.ReaderWriterVirtual;

public interface IToyProtocolReader
{
    Task Read(IToyProtocolHandler handler, CancellationToken cancellationToken);
}
