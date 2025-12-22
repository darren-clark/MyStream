namespace ToyStream.Server;

using Host;
using Protocol;
using Protocol.Messages;
using Protocol.ReaderWriterVirtual;
using Shared;

public sealed class AdminClientVirtualReaderWriterConnection: StreamVirtualReaderWriterConnectionBase
{
    private readonly IServerCore server;

    public AdminClientVirtualReaderWriterConnection(IServerCore server, Stream stream, CancellationToken cancellationToken) : base(stream, cancellationToken)
    {
        this.server = server;
    }

    protected override ValueTask HandleCreateTopicMessage(ref readonly CreateTopicMessage message)
    {
        return this.server.CreateTopicAsync(message.TopicName, message.PartitionCount);
    }

    private ValueTask ThrowNotImplemented() => throw new NotImplementedException();
}