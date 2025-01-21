namespace ToyStream.Server;

using Host;
using Protocol;
using Protocol.Messages;
using Protocol.Responses;

public sealed class AdminClientConnection: StreamConnectionBase
{
    private readonly IServerCore server;

    public AdminClientConnection(IServerCore server, Stream stream, CancellationToken cancellationToken) : base(stream, cancellationToken)
    {
        this.server = server;
    }

    protected override ValueTask HandleCreateTopicMessage(ref readonly CreateTopicMessage message)
    { 
        return SendResponse(new AcknowledgeMessage());
    }

    private ValueTask ThrowNotImplemented() => throw new NotImplementedException();
}