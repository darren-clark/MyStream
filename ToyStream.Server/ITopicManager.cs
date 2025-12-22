namespace ToyStream.Server;

using Shared;

public interface ITopicManager
{
    ValueTask CreateTopicAsync(RefUtf8String topicName, short partitionCount);
}