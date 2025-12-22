namespace ToyStream.Protocol.GenericHandler;

public delegate void ResponseWriter<TResponseMessage>(ref readonly TResponseMessage response)
    where TResponseMessage : struct, IResponseMessage<TResponseMessage>, allows ref struct;

public interface IMessageHandler
{
    public ValueTask HandleMessage<TRequestMessage, TResponseMessage>(
        ref readonly TRequestMessage message,
        ResponseWriter<TResponseMessage> writer)
        where TRequestMessage : struct, IRequestMessage<TRequestMessage, TResponseMessage>, allows ref struct
        where TResponseMessage : struct, IResponseMessage<TResponseMessage>, allows ref struct;

}