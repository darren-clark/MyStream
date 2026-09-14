namespace ToyStream.Protocol;

using Messages;

public interface IToyProtocolHandler
{
    public ValueTask HandleCreateTopicMessage(ref readonly CreateTopicMessage message);
    public ValueTask HandleResponseMessage<TResponseMessage, TResponse>(ref readonly TResponseMessage message) where TResponseMessage : struct, IResponseMessage<TResponseMessage, TResponse>, allows ref struct;
}