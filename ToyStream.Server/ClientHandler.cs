namespace ToyStream.Server;

using Host;
using Protocol;
using Protocol.Receive;
using Protocol.Send;
using CreateTopicMessage = Protocol.Receive.CreateTopicMessage;

public abstract class ClientHandler: ConnectionBase
{
    private readonly IServerCore server;

    public ClientHandler(IServerCore server, IToyProtocolReader reader, IToyProtocolWriter writer, CancellationToken cancellationToken) : base(reader, writer, cancellationToken)
    {
    }

    protected override ValueTask HandleMessageAsync<T>(ref readonly T message)
    {
        return message switch
        {
            CreateTopicMessage createTopicMessage => HandleCreateTopicMessageAsync(createTopicMessage),
            _ => ThrowNotImplemented()
        };
    }


    private ValueTask ThrowNotImplemented() => throw new NotImplementedException();
}