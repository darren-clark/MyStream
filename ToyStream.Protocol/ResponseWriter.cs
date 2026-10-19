using System.Runtime.CompilerServices;
using ToyStream.Protocol.Messages;

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
