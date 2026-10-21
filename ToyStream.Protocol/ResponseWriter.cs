using System.IO.Pipelines;
using System.Runtime.CompilerServices;
using ToyStream.Protocol.Messages;

namespace ToyStream.Protocol;

/// <summary>
/// All a request handler is given to answer with: it can write exactly one response and cannot reach
/// the pipe underneath to flush, complete or cancel it.
/// </summary>
public readonly struct ResponseWriter<TResponse>(PipeWriter writer)
    where TResponse : struct, IResponseMessage<TResponse>, allows ref struct
{
    public void WriteResponse(TResponse message)
    {
        var primitiveWriter = new PrimitiveWriter(writer);
        primitiveWriter.Write(TResponse.Type);
        message.Write(primitiveWriter);
    }

    /// <summary>
    /// A body to follow the header, for a response whose length is not known when it starts. The
    /// caller completes it, and flushing it is what paces a reader that cannot keep up.
    /// </summary>
    public MessageBodyWriter BeginBody() => new(writer);
}

public static class ResponseWriter
{
    [MethodImpl(MethodImplOptions.AggressiveInlining)]
    public static ValueTask AwaitResultAndWriteAcknowledge<TResult>(
        this ResponseWriter<AcknowledgeMessage<TResult>> responseWriter, ValueTask<TResult> task) where TResult : unmanaged, Enum
    {
        if (task.IsCompletedSuccessfully)
        {
            responseWriter.WriteResponse(AcknowledgeMessage.WithResult(task.Result));
            return default;
        }

        return AwaitAndWrite(task, responseWriter);

        [AsyncMethodBuilder(typeof(PoolingAsyncValueTaskMethodBuilder))]
        static async ValueTask AwaitAndWrite(ValueTask<TResult> pending,
            ResponseWriter<AcknowledgeMessage<TResult>> writer)
            => writer.WriteResponse(AcknowledgeMessage.WithResult(await pending.ConfigureAwait(false)));

    }
}
