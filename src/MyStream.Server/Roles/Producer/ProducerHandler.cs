using System.IO.Pipelines;

using MyStream.Protocol;

namespace MyStream.Server.Roles.Producer;

public struct ProducerHandler: IConnectionHandler
{
    public DispatchResult TryParseAndDispatch(MessageType messageType, ref Dispatcher dispatch)
    {
        throw new NotImplementedException();
    }

    public Task<HandshakeStatus> Initialize(PipeReader reader, CancellationToken cancellationToken)
    {
        throw new NotImplementedException();
    }
}