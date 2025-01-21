namespace ToyStream.Protocol;

// Responses need to be captured and passed as a result Task<T>
// Therefore _cannot_ be ref structs and _may_ allocate from a memory pool.
public interface IResponseMessage<T> : IDisposable, IMessage<T> where T : struct, IResponseMessage<T>
{
}