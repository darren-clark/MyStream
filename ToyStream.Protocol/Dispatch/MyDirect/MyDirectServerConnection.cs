using System.Buffers;
using System.IO.Pipelines;
using ToyStream.Protocol.Messages;
using ToyStream.Protocol.Responses;

namespace ToyStream.Protocol.Dispatch;

public class MyDirectServerConnection : MyDirectConnectionBase
{
    // Cached because an instance method group conversion is not cached by the compiler: converting
    // it in the switch arm would allocate a delegate per dispatch.
    private readonly DirectMessageHandler<CreateTopicMessage, AcknowledgeMessage> handleCreateTopic;

    public MyDirectServerConnection(PipeReader reader, PipeWriter writer) : base(reader, writer)
        => this.handleCreateTopic = this.HandleCreateTopic;

    protected override async Task Read(CancellationToken cancellationToken)
    {
            while (!cancellationToken.IsCancellationRequested)
            {
                if (!TryGetMessageType(out var messageType))
                {
                    messageType = await ReadMessageType(cancellationToken).ConfigureAwait(false);
                }
#pragma warning disable CA2012
                await (messageType switch
                {   
                    MessageType.CreateTopic => DispatchRequest(this.handleCreateTopic, cancellationToken),
                    _ => ThrowUnexpected(messageType)
                }).ConfigureAwait(false);
#pragma warning restore CA2012
            }
    }
    
    private void HandleCreateTopic(scoped in CreateTopicMessage message, scoped in ResponseWriter<AcknowledgeMessage> responseWriter)
    {
        responseWriter.WriteResponse(new AcknowledgeMessage());
    }
}