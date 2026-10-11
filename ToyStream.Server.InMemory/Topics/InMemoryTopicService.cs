namespace ToyStream.Server.InMemory;

using ToyStream.Protocol.Responses;
using ToyStream.Shared;

public sealed class InMemoryTopicService : ITopicService
{
    private readonly ConcurrentUtf8Dictionary<InMemoryTopic> topics = new();

    public ValueTask<CreateTopicResult> CreateTopic(RefUtf8String topicName, short partitionCount) =>
        ValueTask.FromResult(this.topics.TryAdd(topicName, new InMemoryTopic(partitionCount))
            ? CreateTopicResult.Success
            : CreateTopicResult.DuplicateTopic);

    public IPartitionWriter? GetPartition(RefUtf8String topicName, short partitionId, out PublishResult failure)
    {
        if (!this.topics.TryGetValue(topicName, out var topic))
        {
            failure = PublishResult.UnknownTopic;
            return null;
        }

        var partition = topic.GetPartition(partitionId);
        failure = partition is null ? PublishResult.UnknownPartition : PublishResult.Success;
        return partition;
    }

    public InMemoryTopic? Find(Utf8String topicName) =>
        this.topics.TryGetValue(topicName, out var topic) ? topic : null;
}
