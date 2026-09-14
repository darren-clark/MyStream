namespace ToyStream.Protocol.Dispatch;

using Messages;

/// <summary>
/// Dispatch variant: the handler is injected as an interface, so reaching it is a virtual call.
/// </summary>
public class GenericHandlerConnection : ConnectionBase, IResponseSender
{
    private readonly IMessageHandler handler;

    public GenericHandlerConnection(
        IToyProtocolReader reader,
        IToyProtocolWriter writer,
        IMessageHandler handler,
        CancellationToken cancellationToken)
        : base(reader, writer, cancellationToken)
    {
        this.handler = handler;
    }

    protected override ValueTask HandleCreateTopicMessage(ref readonly CreateTopicMessage message)
        => this.handler.HandleMessage<CreateTopicMessage, AcknowledgeMessage>(in message, this);

    ValueTask IResponseSender.Send<TResponseMessage>(TResponseMessage response) => this.SendResponse(response);
}
