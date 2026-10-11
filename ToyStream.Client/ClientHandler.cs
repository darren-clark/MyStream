namespace ToyStream.Client;

using System.IO.Pipelines;
using ToyStream.Protocol;
using ToyStream.Protocol.Messages;
using ToyStream.Protocol.Responses;

/// <summary>
/// Client role: its switch is exactly the response set, so receiving a request is a protocol
/// violation rather than an unimplemented handler.
/// </summary>
public readonly struct ClientHandler(PendingRequests pending) : IConnectionHandler
{
    public DispatchResult TryParseAndDispatch(MessageType messageType, ref PrimitiveReader reader,
        PrimitiveWriter writer, CancellationToken cancellationToken) =>
        messageType switch
        {
            MessageType.Acknowledge => ConnectionHandler
                .TryParseAndCompleteResponse<AcknowledgeMessage, AcknowledgeResponse>(pending, ref reader),
            _ => ConnectionHandler.ThrowUnexpectedMessage(messageType)
        };
}
