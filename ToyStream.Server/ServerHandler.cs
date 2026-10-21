using System.IO.Pipelines;
using System.Net;
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

    ValueTask IRequestHandler<CreateTopicMessage, AcknowledgeMessage<CreateTopicResult>>.DispatchRequest(
        CreateTopicMessage message,
        ResponseWriter<AcknowledgeMessage<CreateTopicResult>> responseWriter,
        CancellationToken cancellationToken)
        => responseWriter.AwaitResultAndWriteAcknowledge(
            topicService.CreateTopic(message.TopicName, message.PartitionCount));

    ValueTask IStreamingRequestHandler<PublishMessage, AcknowledgeMessage<PublishResult>>.DispatchRequest(
        StreamingMessage<PublishMessage> message,
        ResponseWriter<AcknowledgeMessage<PublishResult>> responseWriter,
        CancellationToken cancellationToken)
    {
        var header = message.Header;
        if (!topicService.TryStartEventWrite(header.Topic, header.Partition, header.Key, message.Size,
                out var publishResult, out var writer))
        {
            responseWriter.WriteResponse(AcknowledgeMessage.WithResult(publishResult));
            return default;
        }

        return WriteAndAcknowledge(message.Body, writer, responseWriter, cancellationToken);

        static async ValueTask WriteAndAcknowledge(PipeReader body, IEventWriter writer,
            ResponseWriter<AcknowledgeMessage<PublishResult>> responseWriter, CancellationToken cancellationToken)
        {
            var result = await writer.WriteEvent(body, cancellationToken).ConfigureAwait(false);
            responseWriter.WriteResponse(AcknowledgeMessage.WithResult(result));
        }
    }
}