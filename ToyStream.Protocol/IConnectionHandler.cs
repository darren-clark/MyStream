using System.IO.Pipelines;

namespace ToyStream.Protocol;

/// <summary>
/// One role's half of the protocol: the switch over the message types that role serves, plus the
/// handlers for them. Implemented by a struct so <see cref="Connection{THandler}"/> specialises per
/// role and the dispatch call devirtualises.
/// </summary>
public interface IConnectionHandler
{
    /// <summary>
    /// The connection has already consumed the message type byte, so <paramref name="reader"/> starts at
    /// the body. On success it is left past the message; on failure the connection rewinds, so an
    /// unsuccessful result need only report its shortfall. The writer is by value because, unlike the
    /// reader, it carries no position for the caller to observe. The body reader is only meaningful to
    /// message types that carry a payload, and is primed by the dispatch helper that uses it.
    /// </summary>
    DispatchResult TryParseAndDispatch(MessageType messageType, ref PrimitiveReader reader,
        PrimitiveWriter writer, MessageBodyReader body, CancellationToken cancellationToken);
}
