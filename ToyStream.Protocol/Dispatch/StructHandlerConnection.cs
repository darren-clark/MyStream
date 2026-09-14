namespace ToyStream.Protocol.Dispatch;

using Messages;

/// <summary>
/// Dispatch variant: the handler is a struct type argument, so the call is a constrained call the
/// JIT resolves statically. Unlike a class type argument the instantiation is not shared, which is
/// what makes devirtualization possible.
/// </summary>
public class StructHandlerConnection<THandler> : ConnectionBase, IResponseSender
    where THandler : struct, IMessageHandler
{
    // Deliberately not readonly: a readonly field of an unconstrained struct type forces a
    // defensive copy on every call, which is exactly what this variant is trying to avoid.
    private THandler handler;

    public StructHandlerConnection(
        IToyProtocolReader reader,
        IToyProtocolWriter writer,
        THandler handler,
        CancellationToken cancellationToken)
        : base(reader, writer, cancellationToken)
    {
        this.handler = handler;
    }

    protected override ValueTask HandleCreateTopicMessage(ref readonly CreateTopicMessage message)
        => this.handler.HandleMessage<CreateTopicMessage, AcknowledgeMessage>(in message, this);

    ValueTask IResponseSender.Send<TResponseMessage>(TResponseMessage response) => this.SendResponse(response);
}
