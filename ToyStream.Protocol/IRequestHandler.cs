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
