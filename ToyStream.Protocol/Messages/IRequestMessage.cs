namespace ToyStream.Protocol;

public interface IRequestMessage<TSelf, TResponseMessage, TResponse> : IMessage<TSelf>
    where TSelf : struct, IRequestMessage<TSelf, TResponseMessage, TResponse>, allows ref struct
    where TResponseMessage : struct, IResponseMessage<TResponseMessage, TResponse>, allows ref struct
    where TResponse : class;
    
