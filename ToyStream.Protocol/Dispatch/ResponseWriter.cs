using System.IO.Pipelines;

namespace ToyStream.Protocol.Dispatch;

public readonly struct ResponseWriter<TResponse>(PipeWriter writer) where TResponse : struct, IResponseMessage<TResponse>, allows ref struct
{
    public void WriteResponse(TResponse message) 
    {
        PrimitiveWriter<PipeWriter> primitiveWriter = new(writer);
        primitiveWriter.Write(TResponse.Type);
        message.Write(primitiveWriter);
    }
}