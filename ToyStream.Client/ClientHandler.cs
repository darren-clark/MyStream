namespace ToyStream.Client;

using ToyStream.Protocol;

/// <summary>
/// Client role. Responses are matched by the connection to the request that is outstanding, so this
/// switch is only for server-initiated requests - of which there are none yet.
/// </summary>
internal readonly struct ClientHandler : IConnectionHandler
{
    public DispatchResult TryParseAndDispatch(MessageType messageType, ref Dispatcher dispatch) =>
        dispatch.Unexpected(messageType);
}
