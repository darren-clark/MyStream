using System.IO.Pipelines;
using ToyStream.Protocol;
using ToyStream.Protocol.Messages;
using ToyStream.Protocol.Responses;

namespace ToyStream.Server;

/// <summary>
/// Server role: its switch is exactly the request set, and implementing IRequestHandler for each arm
/// is what makes a missing handler a compile error.
/// </summary>
public readonly struct ServerHandler(ITopicService topicService):
    IConnectionHandler,
    IRequestHandler<CreateTopicMessage, AcknowledgeMessage<CreateTopicResult>>,
    IStreamingRequestHandler<PublishMessage, AcknowledgeMessage<PublishResult>>
{
    public DispatchResult TryParseAndDispatch(MessageType messageType, ref PrimitiveReader reader,
        PrimitiveWriter writer, MessageBodyReader body, CancellationToken cancellationToken) =>
        messageType switch
        {
            MessageType.CreateTopic => ConnectionHandler
                .TryParseAndDispatchRequest<ServerHandler, CreateTopicMessage, AcknowledgeMessage<CreateTopicResult>>(this, writer,
                    ref reader, cancellationToken),
            MessageType.PublishEvent => ConnectionHandler
                .TryParseAndDispatchStreamingRequest<ServerHandler, PublishMessage, AcknowledgeMessage<PublishResult>>(this, writer,
                    ref reader, body, cancellationToken),
            _ => ConnectionHandler.ThrowUnexpectedMessage(messageType)
        };

    public ValueTask DispatchRequest(CreateTopicMessage message,
        ResponseWriter<AcknowledgeMessage<CreateTopicResult>> responseWriter,
        CancellationToken cancellationToken)
        => ConnectionHandler.AwaitResultAndWriteAcknowledge(
            topicService.CreateTopic(message.TopicName, message.PartitionCount), responseWriter);

    public ValueTask DispatchRequest(StreamingMessage<PublishMessage> message,
        ResponseWriter<AcknowledgeMessage<PublishResult>> responseWriter,
        CancellationToken cancellationToken)
    {
        // Resolve now: Topic is a view over a buffer the first await releases.
        var header = message.Header;
        var partition = topicService.GetPartition(header.Topic, header.Partition, out var failure);
        if (partition is null)
        {
            // The body is left unread; the connection drains it.
            responseWriter.WriteResponse(AcknowledgeMessage.WithResult(failure));
            return default;
        }

        return AppendAndAcknowledge(partition, message.Body, responseWriter, cancellationToken);
    }

    private static async ValueTask AppendAndAcknowledge(IPartitionWriter partition, PipeReader body,
        ResponseWriter<AcknowledgeMessage<PublishResult>> responseWriter,
        CancellationToken cancellationToken)
    {
        while (true)
        {
            var result = await body.ReadAsync(cancellationToken).ConfigureAwait(false);
            if (result.Buffer.IsEmpty)
            {
                break;
            }

            await partition.AppendAsync(result.Buffer, cancellationToken).ConfigureAwait(false);
            body.AdvanceTo(result.Buffer.End);
        }

        var committed = await partition.CommitAsync(cancellationToken).ConfigureAwait(false);
        responseWriter.WriteResponse(AcknowledgeMessage.WithResult(committed));
    }
}