namespace ToyStream.Protocol.Dispatch;

/// <summary>
/// Lets a handler write a response without holding a reference to the connection's writer,
/// so no closure is needed. Implemented by the connection; passing 'this' costs nothing.
/// </summary>
public interface IResponseSender
{
    ValueTask Send<TResponseMessage>(TResponseMessage response)
        where TResponseMessage : struct, IResponseMessage<TResponseMessage>, allows ref struct;
}

/// <summary>
/// One generic method for every message type, rather than a method per type.
/// </summary>
public interface IMessageHandler
{
    ValueTask HandleMessage<TRequestMessage, TResponseMessage>(
        ref readonly TRequestMessage message,
        IResponseSender sender)
        where TRequestMessage : struct, IRequestMessage<TRequestMessage, TResponseMessage>, allows ref struct
        where TResponseMessage : struct, IResponseMessage<TResponseMessage>, allows ref struct;
}
