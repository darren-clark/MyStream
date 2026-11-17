using System.Diagnostics.CodeAnalysis;
using System.IO.Pipelines;

namespace MyStream.Protocol;

/// <summary>
/// One role's half of the protocol: the switch over the message types that role serves, plus the
/// handlers for them. Implemented by a struct so <see cref="ConnectionBase"/> specialises per
/// role and the dispatch call devirtualises.
/// </summary>
public interface IConnectionHandler
{
    /// <summary>
    /// The connection has already consumed the message type byte, so the dispatcher's reader starts at
    /// the body. On success it is left past the message; on failure the connection rewinds, so an
    /// unsuccessful result need only report its shortfall.
    /// </summary>
    DispatchResult TryParseAndDispatch(MessageType messageType, ref Dispatcher dispatch);
    Task<HandshakeStatus> Initialize(PipeReader reader, CancellationToken cancellationToken);
}
