using System.Buffers;
using System.IO.Pipelines;
using System.Runtime.CompilerServices;
using System.Text.RegularExpressions;

using MyStream.Protocol;
using MyStream.Protocol.Messages;
using MyStream.Protocol.Primitives;
using MyStream.Server.ConsumerGroups;
using MyStream.Server.Topics;

namespace MyStream.Server.Roles.Consumer;

internal sealed class ConsumerConnection
{
    private readonly ITopicService topicService;
    private readonly IConsumerGroupService consumerGroupService;

    private readonly IEventWaiter eventWaiter;

    private readonly List<ITopic> topics = new();

    private const short MAX_MESSAGES = 64;
    
    public ConsumerConnection(ITopicService topicService, IConsumerGroupService consumerGroupService)
    {
        this.topicService = topicService;
        this.consumerGroupService = consumerGroupService;
        this.eventWaiter = topicService.GetEventWaiter();
        
        // Cache handler delegates
        this.InitHandler = Init;
        this.FetchHandler = Fetch;
    }

    public RequestHandler<ConsumerInitRequestMessage, ConsumerInitResponseMessage> InitHandler { get; }
    public RequestWithStreamingResponseHandler<FetchEventRequestMessage, FetchEventResponseMessage> FetchHandler { get; }

    private ValueTask Init(ConsumerInitRequestMessage request, ResponseWriter<ConsumerInitResponseMessage> responseWriter, CancellationToken cancellationToken)
    {
        topics.Clear();
        var partitionCount = 0;
        foreach (var topicName in request.Topics)
        {
            if (!topicService.TryGetTopic(topicName, out var topic))
            {
                responseWriter.WriteResponse(ConsumerInitResponseMessage.InvalidTopic());
                return default;
            }
            partitionCount += topic.PartitionCount;
            topics.Add(topic);
        }
        
        Span<ConsumerInitPartition> results = stackalloc ConsumerInitPartition[partitionCount];
        var resultIndex = 0;
        for (var i =0; i < topics.Count; i++)
        {
            var topic = topics[i];
            foreach (var partition in topic.GetPartitions())
            {
                var partitionDescription = partition.Describe();
                if (!consumerGroupService.TryGetCommittedOffset(request.ConsumerGroup, 
                        topic.Name,
                        partition.PartitionId, out long offset) || !partitionDescription.IsValidOffset(offset))
                {
                    offset = request.OffsetReset switch
                    {
                        OffsetReset.Earliest => partitionDescription.StartOffset,
                        OffsetReset.Latest => partitionDescription.EndOffset,
                        _ => ThrowHelpers.ThrowArgumentOutOfRangeException<long>()
                    };
                }

                results[resultIndex++] = new ConsumerInitPartition
                {
                    TopicIndex = (short)i, Partition = partition.PartitionId, InitialOffset = offset,
                };
            }
        }
        responseWriter.WriteResponse(ConsumerInitResponseMessage.Success(results));
        return default;
    }


   
    private ValueTask Fetch(FetchEventRequestMessage request, StreamingResponseWriter<FetchEventResponseMessage> responseWriter, CancellationToken cancellationToken)
    {
        // IDEA:
        // Rent an array pool of fetched messages and populate
        // Still doesn't resolve the multiple dictionary lookups.
        long fetchableCount = 0;
        Span<FetchOffset> offsets = stackalloc FetchOffset[request.FetchOffsets.Count];
        FetchedEvent[] fetchedEvents = new FetchedEvent[request.FetchOffsets.Count];
        Span<FetchOffset> available =  stackalloc FetchOffset[MAX_MESSAGES];
        short messageCount = 0;
        request.FetchOffsets.CopyTo(offsets);
        // Find available events, distributing evenly(ish) over partitions
        // TODO: Maybe benchmark how much pain is involved in repeated dictionary lookups when we have many messages.
        // Not sure what to do about it thouh
        while (offsets.Length > 0 && messageCount < MAX_MESSAGES)
        {
            for (int i = 0; i < offsets.Length; i++)
            {
                ref readonly var offset = ref offsets[i];
                if (offset.TopicIndex <0 || offset.TopicIndex >= topics.Count)
                {
                    responseWriter.WriteHeader(FetchEventResponseMessage.InvalidTopic());
                    return default;
                }
                
                var topic = topics[offset.TopicIndex];
                
                if (!topic.TryGetPartition(offset.Partition, out var partition))
                {
                    responseWriter.WriteHeader(FetchEventResponseMessage.InvalidPartition());
                    return default;
                }
                var description = partition.Describe();
                if (!description.IsValidOffset(offset.Offset))
                {
                    responseWriter.WriteHeader(FetchEventResponseMessage.InvalidOffset());
                    return default;
                }

                if (offset.Offset < description.EndOffset)
                {
                    if (!partition.TryReadEvent(offset.Offset, out _))
                    fetchedEvents[messageCount++] = partition.TryReadEvent()
                    available[messageCount++] = offset;
                    offsets[i] = new FetchOffset(offset.TopicIndex, offset.Partition, offset.Offset+1);
                }
                else
                {
                    offsets = offsets.RemoveAt(i);
                }
            }
        }
        
        foreach (var offset in request.FetchOffsets)
        {
            if (!topics.TryGetValue(offset.Partition, out var topic))
            {
                responseWriter.WriteHeader(FetchEventResponseMessage.InvalidTopic());
                return default;
            }

            if (!topic.TryGetPartition(offset.Partition, out var partition))
            {
                responseWriter.WriteHeader(FetchEventResponseMessage.InvalidPartition());
                return default;
            }
            
            var description = partition.Describe();
            if (!description.IsValidOffset(offset.Offset))
            {
                responseWriter.WriteHeader(FetchEventResponseMessage.InvalidOffset());
                return default;
            }

            fetchableCount += description.EndOffset - offset.Offset;
        }
        
        short toFetch = (short) Math.Min(MAX_MESSAGES, fetchableCount);
        if (toFetch > 0)
        {
            
            var writer = responseWriter.WriteHeader(FetchEventResponseMessage.Success(toFetch));
            return WriteEvents(toFetch, request, responseWriter, cancellationToken);
        }
        
        return WaitAndWriteEvents(request,  cancellationToken);
    }

    private ValueTask WriteEvents(short toFetch, FetchEventRequestMessage requestMessage, StreamingResponseWriter<FetchEventResponseMessage> responseWriter, CancellationToken cancellationToken)
    {
        var writer = responseWriter.WriteHeader(FetchEventResponseMessage.Success(toFetch));
        var @events = ArrayPool<FetchedEvent>.Shared.Rent(toFetch);
        while (toFetch > 0)
        {
                
        }
    }



}