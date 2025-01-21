namespace ToyStream.Server;

public interface ITopicManager
{
    ValueTask CreateTopicAsync(string topicName, short partitionCount);
}