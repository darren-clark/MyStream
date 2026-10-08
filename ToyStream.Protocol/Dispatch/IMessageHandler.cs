namespace ToyStream.Protocol.Dispatch;

/// <summary>
/// One generic method for every message type, rather than a method per type.
/// Handlers do not write the response: the connection does that with identical code in every
/// variant, so the call that reaches the handler is the only thing that differs.
/// </summary>
public interface IMessageHandler
{
    ValueTask HandleMessage<TRequestMessage, TResponseMessage>(ref readonly TRequestMessage message)
        where TRequestMessage : struct, IRequestMessage<TRequestMessage, TResponseMessage>, allows ref struct
        where TResponseMessage : struct, IResponseMessage<TResponseMessage>, allows ref struct;
}

public interface IMessageHandler<TRequestMessage, TResponseMessage>
    where TRequestMessage : struct, IRequestMessage<TRequestMessage, TResponseMessage>, allows ref struct
    where TResponseMessage : struct, IResponseMessage<TResponseMessage>, allows ref struct
{
    ValueTask HandleRequest(ref readonly TRequestMessage message, ResponseWriter<TResponseMessage> responseWriter);
}