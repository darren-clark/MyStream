using ToyStream.Protocol.Responses;

namespace ToyStream.Protocol;

using Messages;

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
