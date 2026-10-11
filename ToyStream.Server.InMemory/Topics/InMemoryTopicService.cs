using System.Collections.Concurrent;
using ToyStream.Protocol.Responses;
using ToyStream.Shared;

namespace ToyStream.Server.InMemory;

internal class InMemoryTopicService: ITopicService
{
    private readonly ConcurrentUtf8Dictionary<InMemoryTopic> topics = new();
    public ValueTask<CreateTopicResult> CreateTopic(RefUtf8String topicName, short partitionCount)
    {
        if (topics.TryGetValue(topicName, out _))
            return ValueTask.FromResult(CreateTopicResult.DuplicateTopic);
        if (!topics.TryAdd(topicName, new InMemoryTopic(partitionCount)))
            return ValueTask.FromResult(CreateTopicResult.DuplicateTopic);
        return ValueTask.FromResult(CreateTopicResult.Success);
    }
}