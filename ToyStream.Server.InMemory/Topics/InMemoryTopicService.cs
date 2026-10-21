using System.Diagnostics.CodeAnalysis;

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

    public bool TryStartEventWrite(RefUtf8String topicName, short partitionId, RefBytes key, long? size,
        out PublishResult failure,
        [NotNullWhen(true)] out IEventWriter? writer)
    {
        if (!this.topics.TryGetValue(topicName, out var topic))
        {
            failure = PublishResult.UnknownTopic;
            writer = null;
            return false;
        }

        if (!topic.TryGetPartition(partitionId, out var partition))
        {
            failure = PublishResult.UnknownPartition;
            writer = null;
            return false;
        }

        failure = default;
        writer = partition.GetEventWriter(key, size);
        return true;
    }
}