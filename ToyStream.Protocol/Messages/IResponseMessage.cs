namespace ToyStream.Protocol;

// Lets SendResponse constrain to "is a response" without naming TResponse, which it cannot infer.
public interface IResponseMessage<TSelf> : IMessage<TSelf> where TSelf : struct, IMessage<TSelf>, allows ref struct;

// The wire message is a ref struct over the pipe buffer; Response is the heap projection
// handed to the caller via Task<TResponse>.
public interface IResponseMessage<TSelf, out TResponse> : IResponseMessage<TSelf> where TSelf : struct, IResponseMessage<TSelf, TResponse>, allows ref struct
{
    TResponse Response { get; }
}