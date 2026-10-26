using ToyStream.Protocol.Responses;

namespace ToyStream.Protocol;

using Messages;

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
