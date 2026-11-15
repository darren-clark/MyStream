using System.Diagnostics.CodeAnalysis;

namespace MyStream.Server.Topics;

public interface ITopicService
{
    ValueTask<bool> TryCreateTopic(RefUtf8String messageTopicName, short messagePartitionCount);
    bool TryGetTopic(RefUtf8String topicName, [NotNullWhen(true)] out ITopic? topic);
}