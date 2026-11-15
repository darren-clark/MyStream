using MyStream.Protocol.Messages;

namespace MyStream.Protocol;

/// <summary>
/// Implemented by a connection handler once per request type it serves. The struct constraint on the
/// dispatcher is what makes a missing implementation a compile error rather than a runtime one.
/// </summary>
public interface IRequestHandler<in TRequestMessage, TResponseMessage>
    where TRequestMessage : struct, IRequestMessage<TRequestMessage, TResponseMessage>, allows ref struct
    where TResponseMessage : struct, IResponseMessage<TResponseMessage>, allows ref struct
{
    ValueTask DispatchRequest(TRequestMessage message,
        ResponseWriter<TResponseMessage> responseWriter, CancellationToken cancellationToken);
}

public interface IRequestHandler<in TRequestMessage>
    where TRequestMessage : struct, IRequestMessage<TRequestMessage>, allows ref struct
{
    ValueTask DispatchRequest(TRequestMessage message, CancellationToken cancellationToken);
}

public interface IStreamingRequestHandler<TRequestMessage>
    where TRequestMessage : struct, IStreamingRequestMessage<TRequestMessage>, allows ref struct
{
    /// <summary>
    /// The header is a ref struct, so anything read from it that outlives the first await must be
    /// resolved before handing off to an async method.
    /// </summary>
    ValueTask DispatchRequest(StreamingMessage<TRequestMessage> message, CancellationToken cancellationToken);
}

/// <summary>
/// Implemented once per streaming request type a connection handler serves. Identical to
/// <see cref="IRequestHandler{TRequestMessage,TResponseMessage}"/> except that the message carries a
/// body the handler reads before returning.
/// </summary>
public interface IStreamingRequestHandler<TRequestMessage, TResponseMessage>
    where TRequestMessage : struct, IStreamingRequestMessage<TRequestMessage, TResponseMessage>, allows ref struct
    where TResponseMessage : struct, IResponseMessage<TResponseMessage>, allows ref struct
{
    /// <summary>
    /// The header is a ref struct, so anything read from it that outlives the first await - a topic
    /// name is a view over pipe memory - must be resolved before handing off to an async method.
    /// </summary>
    ValueTask DispatchRequest(StreamingMessage<TRequestMessage> message,
        ResponseWriter<TResponseMessage> responseWriter, CancellationToken cancellationToken);
}
