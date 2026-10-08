using System.IO.Pipelines;
using ToyStream.Protocol.Messages;
using ToyStream.Protocol.Responses;

namespace ToyStream.Protocol.Dispatch;

public class MyDirectClientConnection(PipeReader reader, PipeWriter writer) : MyDirectConnectionBase(reader, writer)
{
    protected override async Task Read(CancellationToken cancellationToken)
    {
        while (!cancellationToken.IsCancellationRequested)
        {
            var messageType = await ReadMessageType(cancellationToken).ConfigureAwait(false);
#pragma warning disable CA2012
            await (messageType switch
            {   
                MessageType.Acknowledge => HandleResponse<AcknowledgeMessage, AcknowledgeResponse>(cancellationToken),
                _ => ThrowUnexpected(messageType)
            }).ConfigureAwait(false);
#pragma warning restore CA2012
        }        
    }
}