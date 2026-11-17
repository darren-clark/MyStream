using System.IO.Pipelines;

using MyStream.Protocol;
using MyStream.Protocol.Messages;

namespace MyStream.Server.Roles.Producer;

internal struct ProducerHandler(ProducerConnection connection): IConnectionHandler
{
    public DispatchResult TryParseAndDispatch(MessageType messageType, ref Dispatcher dispatch)
        => messageType switch
        {
            MessageType.InitProducer => dispatch.Request(connection.InitHandler),
            _ => dispatch.Unexpected(messageType)
        };

    public Task<HandshakeStatus> Initialize(PipeReader reader, CancellationToken cancellationToken)
    {
        throw new NotImplementedException();
    }
}