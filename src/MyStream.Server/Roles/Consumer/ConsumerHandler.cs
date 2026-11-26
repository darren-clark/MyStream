using System.IO.Pipelines;

using MyStream.Protocol;

namespace MyStream.Server.Roles.Consumer;

internal struct ConsumerHandler(ConsumerConnection connection): IConnectionHandler
{
    public DispatchResult TryParseAndDispatch(MessageType messageType, ref Dispatcher dispatch)
        => messageType switch
        {
            MessageType.InitConsumer => dispatch.Request(connection.InitHandler),
//            MessageType.Fetch => dispatch.Reque
            _ => dispatch.Unexpected(messageType)
        };


    public Task<HandshakeStatus> Initialize(PipeReader reader, CancellationToken cancellationToken)
    {
        throw new NotImplementedException();
    }
}