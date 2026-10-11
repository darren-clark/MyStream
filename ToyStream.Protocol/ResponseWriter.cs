namespace ToyStream.Protocol;

/// <summary>
/// All a request handler is given to answer with: it can write exactly one response and cannot reach
/// the pipe underneath to flush, complete or cancel it.
/// </summary>
public readonly struct ResponseWriter<TResponse>(PrimitiveWriter writer)
    where TResponse : struct, IResponseMessage<TResponse>, allows ref struct
{
    public void WriteResponse(TResponse message)
    {
        writer.Write(TResponse.Type);
        message.Write(writer);
    }
}
