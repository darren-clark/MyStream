// namespace ToyStream.Protocol;
//
// public interface IResponseMessage
// {
//     
// }
//
// // Responses need to be captured and passed as a result Task<T>
// // Therefore _cannot_ be ref structs and _may_ allocate from a memory pool.
// public interface IResponseMessage<TMessage, out TResponse> : IResponseMessage, IDisposable
//     where TMessage: struct, IMessage<TMessage>, allows ref struct
//     where TResponse : class, IResponseMessage<TMessage, TResponse>
// {
//     internal static abstract TResponse Box(ref readonly TMessage message);
// }