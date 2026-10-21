using System.Diagnostics.CodeAnalysis;

namespace ToyStream.Server.InMemory;

using ToyStream.Shared;

public sealed class InMemoryTopicService : ITopicService
{
    private readonly ConcurrentUtf8Dictionary<InMemoryTopic> topics = new();

    public ValueTask<bool> TryCreateTopic(RefUtf8String topicName, short partitionCount)
    {
        // Materialised once here so the topic can name itself after the request buffer is gone.
        var name = (Utf8String)topicName;
        return ValueTask.FromResult(this.topics.TryAdd(name, new InMemoryTopic(name, partitionCount)));
    }
    
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