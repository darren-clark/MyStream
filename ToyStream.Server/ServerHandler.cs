using System.IO.Pipelines;
using ToyStream.Protocol;
using ToyStream.Protocol.Messages;
using ToyStream.Protocol.Responses;

namespace ToyStream.Server;

/// <summary>
/// Server role: its switch is exactly the request set, and implementing IRequestHandler for each arm
/// is what makes a missing handler a compile error.
/// </summary>
public readonly struct ServerHandler(ITopicService topicService) :
    IConnectionHandler,
    IRequestHandler<CreateTopicMessage, AcknowledgeMessage<CreateTopicResult>>,
    IStreamingRequestHandler<PublishMessage, AcknowledgeMessage<PublishResult>>
{
    public DispatchResult TryParseAndDispatch(MessageType messageType, ref Dispatcher dispatch) =>
        messageType switch
        {
            MessageType.CreateTopic =>
                dispatch.Request<ServerHandler, CreateTopicMessage, AcknowledgeMessage<CreateTopicResult>>(this),
            MessageType.PublishEvent =>
                dispatch.StreamingRequest<ServerHandler, PublishMessage, AcknowledgeMessage<PublishResult>>(this),
            _ => dispatch.Unexpected(messageType)
        };

    ValueTask IRequestHandler<CreateTopicMessage, AcknowledgeMessage<CreateTopicResult>>.DispatchRequest(CreateTopicMessage message,
        ResponseWriter<AcknowledgeMessage<CreateTopicResult>> responseWriter,
        CancellationToken cancellationToken)
        => responseWriter.AwaitResultAndWriteAcknowledge(
            topicService.CreateTopic(message.TopicName, message.PartitionCount));

     ValueTask IStreamingRequestHandler<PublishMessage, AcknowledgeMessage<PublishResult>>.DispatchRequest(StreamingMessage<PublishMessage> message,
        ResponseWriter<AcknowledgeMessage<PublishResult>> responseWriter,
        CancellationToken cancellationToken)
    {
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