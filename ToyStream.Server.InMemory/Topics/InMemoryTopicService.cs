using System.Diagnostics.CodeAnalysis;

namespace ToyStream.Server.InMemory;

using ToyStream.Shared;

public sealed class InMemoryTopicService : ITopicService
{
    private readonly ConcurrentUtf8Dictionary<InMemoryTopic> topics = new();

    public ValueTask<bool> TryCreateTopic(RefUtf8String topicName, short partitionCount) =>
        ValueTask.FromResult(this.topics.TryAdd(topicName, new InMemoryTopic(partitionCount)));
    
    public bool TryGetTopic(RefUtf8String topicName, [NotNullWhen(true)] out ITopic? topic)
    {
        if (!this.topics.TryGetValue(topicName, out var found))
        {
            topic = null;
            return false;
        }

        topic = found;
        return true;
    }
}