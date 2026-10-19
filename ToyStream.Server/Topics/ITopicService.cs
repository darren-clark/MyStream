using ToyStream.Protocol.Responses;
using ToyStream.Shared;

namespace ToyStream.Server;


public interface ITopicService
{
    ValueTask<CreateTopicResult> CreateTopic(RefUtf8String topicName, short partitionCount);
    
    
    /// <summary>
    /// Resolved while the request header is still buffered, because the topic name is a view over
    /// bytes the connection advances past once streaming starts.
    /// </summary>
    IPartitionWriter? GetPartition(RefUtf8String topicName, short partitionId, out PublishResult failure);
}