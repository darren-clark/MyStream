using System.IO.Pipelines;

using MyStream.Protocol;
using MyStream.Protocol.Messages;
using MyStream.Server.Topics;

namespace MyStream.Server.Roles.Producer;

internal sealed class ProducerConnection
{
    private readonly ITopicService topicService;
    private readonly Dictionary<short, ITopic> topics = new();

    public ProducerConnection(ITopicService topicService)
    {
        this.topicService = topicService;
        InitHandler = Init;
        ProduceEventHandler = ProduceEvent;
    }


    // Cache delegates, the compiler won't do it for instance methods.
    public RequestHandler<ProducerInitRequestMessage, ProducerInitResponseMessage> InitHandler { get; }

    public StreamingRequestHandler<ProduceEventMessage, AcknowledgeMessage<ProduceResult>> ProduceEventHandler { get; }

    private ValueTask Init(ProducerInitRequestMessage message, ResponseWriter<ProducerInitResponseMessage> responseWriter,
        CancellationToken cancellationToken)
    {
        topics.Clear();
        int partitionCounts = 0;
        short topicIndex = 0;
        foreach (var topicName in message.Topics)
        {
            if (!topicService.TryGetTopic(topicName, out var topic))
            {
                responseWriter.WriteResponse(ProducerInitResponseMessage.UnknownTopic());
                return default;
            }

            topics[topicIndex++] = topic;
            partitionCounts += topic.GetPartitions().Count;
        }

        Span<OwnedPartition> ownedPartitions = stackalloc OwnedPartition[partitionCounts];
        foreach (var topic in topics)
        {
            var ownedIndex = 0;
            foreach (var partition in topic.Value.GetPartitions())
            {
                ownedPartitions[ownedIndex++] = new OwnedPartition(topic.Key, partition.PartitionId);
            }
        }

        var response = new ProducerInitResponseMessage
        {
            Result = InitProducerResult.Success, OwnedPartitions = new(ownedPartitions)
        };
        responseWriter.WriteResponse(response);
        return default;
    }

    private ValueTask ProduceEvent(StreamingMessage<ProduceEventMessage> request,
        ResponseWriter<AcknowledgeMessage<ProduceResult>> responseWriter, CancellationToken cancellationToken)
    {
        var header = request.Header;

        if (!topics.TryGetValue(header.TopicIndex, out var topic))
        {
            responseWriter.WriteResponse(AcknowledgeMessage.WithResult(ProduceResult.InvalidTopic));
            return default;
        }

        if (!topic.TryGetPartition(header.Partition, out var partition))
        {
            responseWriter.WriteResponse(AcknowledgeMessage.WithResult(ProduceResult.InvalidPartition));
            return default;
        }

        return WriteAndAcknowledge(request.Body, partition.GetEventWriter(header.Key, request.Size),
            responseWriter, cancellationToken);

        static async ValueTask WriteAndAcknowledge(PipeReader body, IEventWriter writer,
            ResponseWriter<AcknowledgeMessage<ProduceResult>> responseWriter, CancellationToken cancellationToken)
        {
            await writer.WriteEvent(body, cancellationToken).ConfigureAwait(false);
            responseWriter.WriteResponse(AcknowledgeMessage.WithResult(ProduceResult.Success));
        }
    }
}