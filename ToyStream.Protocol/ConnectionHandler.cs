using System.Buffers;
using System.Diagnostics;
using System.Diagnostics.CodeAnalysis;
using System.IO.Pipelines;

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

    public static DispatchResult TryParseAndCompleteResponse<TResponseMessage, TResponse>(
        PendingRequests pending, ref PrimitiveReader reader)
        where TResponseMessage : struct, IResponseMessage<TResponseMessage>, allows ref struct
        where TResponse : class, IResponse<TResponseMessage, TResponse>
    {
        if (!TResponseMessage.TryRead(ref reader, out var response, out var bytesNeeded))
        {
            return DispatchResult.Insufficient(bytesNeeded);
        }

        pending.CompleteResponse<TResponseMessage, TResponse>(in response);
        return DispatchResult.Dispatched(default, wroteResponse: false);
    }
}
