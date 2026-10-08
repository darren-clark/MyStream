namespace ToyStream.Protocol.Dispatch.Context;

using System.Buffers;
using System.IO.Pipelines;
using Messages;
using Responses;

/// <summary>
/// Client role: its switch is exactly the response set, so receiving a request is a protocol
/// violation rather than an unimplemented handler.
/// </summary>
public readonly struct ClientContext(ResponseTracker tracker) : IConnectionContext
{
    public DispatchResult TryParseAndDispatch(ref ReadOnlySequence<byte> buffer, ref int bytesNeeded,
        PipeWriter writer, CancellationToken cancellationToken)
    {
        if (buffer.IsEmpty)
        {
            bytesNeeded = 1;
            return DispatchResult.Insufficient;
        }

        var messageType = (MessageType) buffer.FirstSpan[0];

        switch (messageType)
        {
            case MessageType.Acknowledge:
                return MessageDispatcher.TryParseAndCompleteResponse<AcknowledgeMessage, AcknowledgeResponse>(
                    tracker, ref buffer, ref bytesNeeded);
            default:
                return MessageDispatcher.ThrowUnexpectedMessage(messageType);
        }
    }
}
