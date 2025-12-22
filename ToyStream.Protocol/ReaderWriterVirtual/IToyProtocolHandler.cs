namespace ToyStream.Protocol.ReaderWriterVirtual;

using ToyStream.Protocol.Messages;

public interface IToyProtocolHandler
{
    public ValueTask HandleCreateTopicMessage(ref readonly CreateTopicMessage message);
    public ValueTask HandleResponseMessage<TResponseMessage, TResponse>(ref readonly TResponseMessage message) 
        where TResponseMessage : struct, IResponseMessage<TResponseMessage>, allows ref struct
        where TResponse : class, IResponse<TResponseMessage, TResponse>;
}