using System.Diagnostics.CodeAnalysis;

using MyStream.Protocol.Primitives;
using MyStream.Server.Topics;

namespace MyStream.Server.InMemory.Topics;

internal class InMemoryTopicService: ITopicService
{
    public IEventWaiter GetEventWaiter()
    {
        throw new NotImplementedException();
    }
    
    public ValueTask<bool> TryCreateTopic(RefUtf8String messageTopicName, short messagePartitionCount)
    {
        throw new NotImplementedException();
    }

    public bool TryGetTopic(RefUtf8String topicName, [NotNullWhen(true)] out ITopic? topic)
    {
        throw new NotImplementedException();
    }
}