using System.Diagnostics;
using System.Diagnostics.CodeAnalysis;
using System.Runtime.CompilerServices;
using ToyStream.Protocol.Messages;

namespace ToyStream.Protocol;

// Helpers for IConnectionHandler implementations
public static class ConnectionHandler
{
    /// <summary>Keeps the interpolated string and exception construction out of the caller's IL budget.</summary>
    [DoesNotReturn]
    [StackTraceHidden]
    public static DispatchResult ThrowUnexpectedMessage(MessageType messageType) =>
        throw new InvalidOperationException($"Received unexpected message type {messageType}.");

    /// <summary>The connection has already consumed the message type byte, so the reader starts at the body.</summary>
    public static DispatchResult TryParseAndDispatchRequest<THandler, TRequest, TResponse>(THandler handler,
        PrimitiveWriter writer, ref PrimitiveReader reader, CancellationToken cancellationToken)
        where TRequest: struct, IRequestMessage<TRequest, TResponse>, allows ref struct
        where THandler : struct, IRequestHandler<TRequest, TResponse>
        where TResponse : struct, IResponseMessage<TResponse>, allows ref struct
    {
        if (!TRequest.TryRead(ref reader, out var request, out var bytesNeeded))
        {
            return DispatchResult.Insufficient(bytesNeeded);
        }

        var responseWriter = new ResponseWriter<TResponse>(writer);
        return DispatchResult.Dispatched(handler.DispatchRequest(request, responseWriter, cancellationToken));
    }
    
    /// <summary>
    /// Parses a message header without dispatching it, for handlers that stream a body afterwards.
    /// The reader must be the caller's local: the message is a view over it.
    /// </summary>
    public static bool TryParseRequest<TRequest>(ref PrimitiveReader reader, out TRequest message,
        out int bytesNeeded)
        where TRequest : struct, IMessage<TRequest>, allows ref struct
        => TRequest.TryRead(ref reader, out message, out bytesNeeded);

    /// <summary>
    /// As <see cref="TryParseAndDispatchRequest{THandler,TRequest,TResponse}"/>, but the message is
    /// given a body to read. The connection owns the body either way: whatever the handler leaves is
    /// drained before the next message, so the handler cannot desynchronise the stream.
    /// </summary>
    public static DispatchResult TryParseAndDispatchStreamingRequest<THandler, TRequest, TResponse>(
        THandler handler, PrimitiveWriter writer, ref PrimitiveReader reader, MessageBodyReader body,
        CancellationToken cancellationToken)
        where TRequest : struct, IStreamingRequestMessage<TRequest, TResponse>, allows ref struct
        where THandler : struct, IStreamingRequestHandler<TRequest, TResponse>
        where TResponse : struct, IResponseMessage<TResponse>, allows ref struct
    {
        if (!TRequest.TryRead(ref reader, out var request, out var bytesNeeded))
        {
            return DispatchResult.Insufficient(bytesNeeded);
        }

        // Read here rather than by the message, mirroring where the send side writes it.
        if (!reader.TryReadInt64(out var declaredSize, out bytesNeeded))
        {
            return DispatchResult.Insufficient(bytesNeeded);
        }

        // Primed before dispatch: the handler may start reading the body synchronously.
        body.Begin(reader.Remaining);

        var message = new StreamingMessage<TRequest>
        {
            Header = request,
            Size = declaredSize < 0 ? null : declaredSize,
            Body = body,
        };

        var responseWriter = new ResponseWriter<TResponse>(writer);
        var dispatched = handler.DispatchRequest(message, responseWriter, cancellationToken);
        return DispatchResult.Streaming(dispatched, message.Size);
    }

    /// <summary>
    /// Non-async so a topic service that answers synchronously - which an in-memory one always does -
    /// costs no second state machine on the per-message path.
    /// </summary>
    public static ValueTask AwaitResultAndWriteAcknowledge<TResult>(ValueTask<TResult> task,
        ResponseWriter<AcknowledgeMessage<TResult>> responseWriter)
        where TResult : unmanaged, Enum
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
