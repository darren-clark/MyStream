using System.Buffers;
using ToyStream.Protocol;
using ToyStream.Protocol.Messages;
using ToyStream.Protocol.Responses;
using ToyStream.Shared;

namespace ToyStream.Server;

public readonly struct DescribeTopicHandler(ITopicService topicService):
    IRequestHandler<DescribeTopicRequestMessage, DescribeTopicResponseMessage>
{
    /// <summary>Roughly a kilobyte of descriptions; beyond this a describe rents instead.</summary>
    private const int MaxStackPartitions = 64;
    
    public ValueTask DispatchRequest(DescribeTopicRequestMessage message, ResponseWriter<DescribeTopicResponseMessage> responseWriter,
        CancellationToken cancellationToken)
    {
        if (!topicService.TryGetTopic(message.TopicName, out var topic))
        {
            responseWriter.WriteResponse(new DescribeTopicResponseMessage
            {
                Result = DescribeTopicResult.UnknownTopic,
                Partitions = default
            });
            return default;
        }

        PartitionDescription[]? rented = null;
        var descriptions = topic.PartitionCount <= MaxStackPartitions
            ? stackalloc PartitionDescription[MaxStackPartitions]
            : (rented = ArrayPool<PartitionDescription>.Shared.Rent(topic.PartitionCount));

        try
        {
            descriptions = descriptions[..topic.PartitionCount];
            topic.Describe(descriptions);

            responseWriter.WriteResponse(new DescribeTopicResponseMessage
            {
                Result = DescribeTopicResult.Success,
                Partitions = new PartitionDescriptions(descriptions)
            });
        }
        finally
        {
            if (rented is not null)
            {
                // The response is already copied into the pipe, so the buffer can go back now.
                ArrayPool<PartitionDescription>.Shared.Return(rented);
            }
        }        
        return default;
    }
}