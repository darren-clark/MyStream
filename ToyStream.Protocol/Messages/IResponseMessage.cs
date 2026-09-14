namespace ToyStream.Protocol;

public interface IResponseMessage<TSelf> : IMessage<TSelf> where TSelf : struct, IResponseMessage<TSelf>, allows ref struct;

public interface IResponse;

// The heap projection of a response message. Built by the awaiting side only, while the
// pipe buffer the response message views is still valid.
public interface IResponse<TResponseMessage, out TResponse> : IResponse
    where TResponseMessage : struct, IResponseMessage<TResponseMessage>, allows ref struct
    where TResponse : class, IResponse<TResponseMessage, TResponse>
{
    internal static abstract TResponse CreateResponse(ref readonly TResponseMessage response);
}