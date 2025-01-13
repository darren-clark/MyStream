namespace ToyStream.Server.Host;

using System.IO.Pipelines;
using Protocol;
using Protocol.Receive;
using Protocol.Send;
using AcknowledgeMessage = Protocol.Send.AcknowledgeMessage;
using CreateTopicMessage = Protocol.Receive.CreateTopicMessage;

public class ClientConnection : ConnectionBase, IAdminClient
{
    private readonly IServerCore server;
    private readonly Stream stream;
    private readonly Guid clientId;
    
    public ClientConnection(IServerCore server, Stream stream, CancellationToken cancellationToken): base(new ToyProtocolReader(PipeReader.Create(stream)), new ToyProtocolWriter(PipeWriter.Create(stream)), cancellationToken)
    {
        this.server = server;
        this.stream = stream;
        this.clientId = Guid.NewGuid();
        server.RegisterClient(this);
    }

    protected override ValueTask HandleMessageAsync<T>(ref readonly T message)
    {
        return message switch
        {
            CreateTopicMessage createTopicMessage => HandleCreateTopicMessageAsync(createTopicMessage),
            _ => ThrowNotImplemented()
        };
    }

    private async ValueTask HandleCreateTopicMessageAsync(CreateTopicMessage createTopicMessage)
    {

        await SendResponse(new AcknowledgeMessage());
    }

    public string ClientId => this.clientId.ToString();
    private static ValueTask ThrowNotImplemented() => throw new NotImplementedException();

    protected override async ValueTask DisposeAsync()
    {
        await base.DisposeAsync();
        server.UnregisterClient(this);
        await stream.DisposeAsync();
    }
}