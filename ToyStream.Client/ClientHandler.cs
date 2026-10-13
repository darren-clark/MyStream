namespace ToyStream.Client;

using System.IO.Pipelines;
using ToyStream.Protocol;

/// <summary>
/// Client role. Responses are matched by the connection to the request that is outstanding, so this
/// switch is only for server-initiated requests - of which there are none yet.
/// </summary>
public readonly struct ClientHandler : IConnectionHandler
{
    public DispatchResult TryParseAndDispatch(MessageType messageType, ref PrimitiveReader reader,
        PrimitiveWriter writer, MessageBodyReader body, CancellationToken cancellationToken) =>
        ConnectionHandler.ThrowUnexpectedMessage(messageType);
}
