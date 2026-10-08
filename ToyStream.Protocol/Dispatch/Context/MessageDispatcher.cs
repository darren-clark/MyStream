using System.Buffers;
using System.Diagnostics;
using System.Diagnostics.CodeAnalysis;
using System.IO.Pipelines;

namespace ToyStream.Protocol.Dispatch.Context;

public interface IRequestHandler<in TRequestMessage, TResponseMessage>
    where TRequestMessage : struct, IRequestMessage<TRequestMessage, TResponseMessage>, allows ref struct
    where TResponseMessage : struct, IResponseMessage<TResponseMessage>, allows ref struct
{
    ValueTask DispatchRequest(TRequestMessage message,
        ResponseWriter<TResponseMessage> responseWriter, CancellationToken cancellationToken);
}

public static class MessageDispatcher
{
    /// <summary>Keeps the interpolated string and exception construction out of the caller's IL budget.</summary>
    [DoesNotReturn]
    [StackTraceHidden]
    public static DispatchResult ThrowUnexpectedMessage(MessageType messageType) =>
        throw new InvalidOperationException($"Received unexpected message type {messageType}.");

    /// <summary>The buffer still carries the message type byte; skipping it here keeps every switch arm one line.</summary>
    public static DispatchResult TryParseAndDispatchRequest<THandler, TRequest, TResponse>(THandler handler,
        PipeWriter writer, ref ReadOnlySequence<byte> buffer, ref int bytesNeeded,
        CancellationToken cancellationToken)
        where TRequest: struct, IRequestMessage<TRequest, TResponse>, allows ref struct
        where THandler : struct, IRequestHandler<TRequest, TResponse>
        where TResponse : struct, IResponseMessage<TResponse>, allows ref struct
    {
        var reader = new PrimitiveReader(buffer.Slice(1));
        if (!TRequest.TryRead(ref reader, out var request, out bytesNeeded))
        {
            return DispatchResult.Insufficient;
        }

        buffer = reader.Remaining;
        var responseWriter = new ResponseWriter<TResponse>(writer);
        return DispatchResult.Dispatched(handler.DispatchRequest(request, responseWriter, cancellationToken));
    }

    public static DispatchResult TryParseAndCompleteResponse<TResponseMessage, TResponse>(
        ResponseTracker tracker, ref ReadOnlySequence<byte> buffer, ref int bytesNeeded)
        where TResponseMessage : struct, IResponseMessage<TResponseMessage>, allows ref struct
        where TResponse : class, IResponse<TResponseMessage, TResponse>
    {
        var reader = new PrimitiveReader(buffer.Slice(1));
        if (!TResponseMessage.TryRead(ref reader, out var response, out bytesNeeded))
        {
            return DispatchResult.Insufficient;
        }

        buffer = reader.Remaining;
        tracker.CompleteResponse<TResponseMessage, TResponse>(in response);
        return DispatchResult.Dispatched(default, wroteResponse: false);
    }
}
