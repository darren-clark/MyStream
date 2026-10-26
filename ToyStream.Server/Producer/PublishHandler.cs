using System.IO.Pipelines;
using ToyStream.Protocol;
using ToyStream.Protocol.Messages;
using ToyStream.Protocol.Responses;
using ToyStream.Shared;

namespace ToyStream.Server;

public readonly struct PublishHandler(ITopicService topicService) :
    IStreamingRequestHandler<PublishMessage, AcknowledgeMessage<PublishResult>>
{
    public ValueTask DispatchRequest(StreamingMessage<PublishMessage> message,
        ResponseWriter<AcknowledgeMessage<PublishResult>> responseWriter,
        CancellationToken cancellationToken)
    {
        var header = message.Header;

        // Composed from lookups rather than one service call, so the service never needs to know
        // which protocol result a missing topic or partition maps to.
        if (!topicService.TryGetTopic(header.Topic, out var topic))
        {
            responseWriter.WriteResponse(AcknowledgeMessage.WithResult(PublishResult.UnknownTopic));
            return default;
        }

        if (!topic.TryGetPartition(header.Partition, out var partition))
        {
            responseWriter.WriteResponse(AcknowledgeMessage.WithResult(PublishResult.UnknownPartition));
            return default;
        }

        return WriteAndAcknowledge(message.Body, partition.GetEventWriter(header.Key, message.Size),
            responseWriter, cancellationToken);

        static async ValueTask WriteAndAcknowledge(PipeReader body, IEventWriter writer,
            ResponseWriter<AcknowledgeMessage<PublishResult>> responseWriter, CancellationToken cancellationToken)
        {
            await writer.WriteEvent(body, cancellationToken).ConfigureAwait(false);
            responseWriter.WriteResponse(AcknowledgeMessage.WithResult(PublishResult.Success));
        }
    }
}
