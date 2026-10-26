using System.Buffers;
using System.IO.Pipelines;
using System.Diagnostics;

namespace ToyStream.Protocol;

using System.Diagnostics.CodeAnalysis;
using ToyStream.Protocol.Messages;

/// <summary>
/// Everything a dispatch needs, bundled so a handler's switch names only the message types it
/// serves. Passed by ref and owns the reader rather than referencing one, because a ref field
/// cannot point at a ref struct. The body is carried whether or not the message has one, so arms
/// that do not stream simply ignore it.
/// </summary>
public ref struct Dispatcher
{
    private PrimitiveReader reader;
    private readonly PipeWriter writer;
    private readonly MessageBodyReader body;
    private readonly CancellationToken cancellationToken;

    internal Dispatcher(PrimitiveReader reader, PipeWriter writer, MessageBodyReader body,
        CancellationToken cancellationToken)
    {
        this.reader = reader;
        this.writer = writer;
        this.body = body;
        this.cancellationToken = cancellationToken;
    }

    internal ReadOnlySequence<byte> Remaining => this.reader.Remaining;

    /// <summary>The connection has already consumed the message type byte, so the reader starts at the body.</summary>
    public DispatchResult Request<THandler, TRequest, TResponse>(THandler handler)
        where TRequest : struct, IRequestMessage<TRequest, TResponse>, allows ref struct
        where THandler : struct, IRequestHandler<TRequest, TResponse>
        where TResponse : struct, IResponseMessage<TResponse>, allows ref struct
    {
        if (!TRequest.TryRead(ref this.reader, out var request, out var bytesNeeded))
        {
            return DispatchResult.Insufficient(bytesNeeded);
        }

        var responseWriter = new ResponseWriter<TResponse>(this.writer);
        return DispatchResult.Dispatched(handler.DispatchRequest(request, responseWriter, this.cancellationToken));
    }
    
    /// <summary>
    /// As <see cref="Request{THandler,TRequest,TResponse}"/>, but the message is given a body to read.
    /// The connection owns the body either way: whatever the handler leaves is drained before the next
    /// message, so the handler cannot desynchronise the stream.
    /// </summary>
    public DispatchResult StreamingRequest<THandler, TRequest, TResponse>(THandler handler)
        where TRequest : struct, IStreamingRequestMessage<TRequest, TResponse>, allows ref struct
        where THandler : struct, IStreamingRequestHandler<TRequest, TResponse>
        where TResponse : struct, IResponseMessage<TResponse>, allows ref struct
    {
        if (!TRequest.TryRead(ref this.reader, out var request, out var bytesNeeded))
        {
            return DispatchResult.Insufficient(bytesNeeded);
        }

        // Read here rather than by the message, mirroring where the send side writes it.
        if (!this.reader.TryReadInt64(out var declaredSize, out bytesNeeded))
        {
            return DispatchResult.Insufficient(bytesNeeded);
        }

        // Primed before dispatch: the handler may start reading the body synchronously.
        this.body.Begin(declaredSize <0 ? null : declaredSize, this.reader.Remaining);

        var message = new StreamingMessage<TRequest>
        {
            Header = request,
            Size= this.body.DeclaredSize,
            Body = this.body,
        };

        var responseWriter = new ResponseWriter<TResponse>(this.writer);
        var dispatched = handler.DispatchRequest(message, responseWriter, this.cancellationToken);
        return DispatchResult.Streaming(dispatched);
    }

    /// <summary>
    /// A message with no answer. The handler is given no <see cref="ResponseWriter{T}"/> at all, so a
    /// role whose connection is one directional cannot write back by accident.
    /// </summary>
    public DispatchResult Request<THandler, TRequest>(THandler handler)
        where TRequest : struct, IRequestMessage<TRequest>, allows ref struct
        where THandler : struct, IRequestHandler<TRequest>
    {
        if (!TRequest.TryRead(ref this.reader, out var request, out var bytesNeeded))
        {
            return DispatchResult.Insufficient(bytesNeeded);
        }

        return DispatchResult.Dispatched(handler.DispatchRequest(request, this.cancellationToken),
            wroteResponse: false);
    }

    /// <summary>
    /// As <see cref="Request{THandler,TRequest}"/>, but the message is given a body to read.
    /// </summary>
    public DispatchResult StreamingRequest<THandler, TRequest>(THandler handler)
        where TRequest : struct, IStreamingRequestMessage<TRequest>, allows ref struct
        where THandler : struct, IStreamingRequestHandler<TRequest>
    {
        if (!TRequest.TryRead(ref this.reader, out var request, out var bytesNeeded))
        {
            return DispatchResult.Insufficient(bytesNeeded);
        }

        // Read here rather than by the message, mirroring where the send side writes it.
        if (!this.reader.TryReadInt64(out var declaredSize, out bytesNeeded))
        {
            return DispatchResult.Insufficient(bytesNeeded);
        }

        // Primed before dispatch: the handler may start reading the body synchronously.
        this.body.Begin(declaredSize < 0 ? null : declaredSize, this.reader.Remaining);

        var message = new StreamingMessage<TRequest>
        {
            Header = request,
            Size = this.body.DeclaredSize,
            Body = this.body,
        };

        return DispatchResult.Streaming(handler.DispatchRequest(message, this.cancellationToken),
            wroteResponse: false);
    }

    [StackTraceHidden]
    [DoesNotReturn]
    public DispatchResult Unexpected(MessageType messageType) =>
        throw new InvalidOperationException($"Received unexpected message type {messageType}.");
}
