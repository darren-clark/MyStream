namespace ToyStream.Protocol.PerMessageHandler;

using Messages;

public delegate void ResponseWriter<TResponseMessage>(ref readonly TResponseMessage response)
    where TResponseMessage : struct, IResponseMessage<TResponseMessage>, allows ref struct;

public interface IMessageHandler
{
    public ValueTask HandleCreateTopicMessage(ref readonly CreateTopicMessage message, ResponseWriter<CreateTopicResponseMessage> writer);   
}