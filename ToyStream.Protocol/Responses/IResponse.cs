namespace ToyStream.Protocol;

// The heap projection of a response message. Built by the awaiting side only, while the
// pipe buffer the response message views is still valid.
public interface IResponse<TResponseMessage, out TResponse>
    where TResponseMessage : struct, IResponseMessage<TResponseMessage>, allows ref struct
    where TResponse : IResponse<TResponseMessage, TResponse>
{
    internal static abstract TResponse CreateResponse(ref readonly TResponseMessage response);
}
