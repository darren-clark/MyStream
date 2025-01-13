namespace ToyStream.Server;

public interface ITopicManager
{
    ValueTask AddTopicAsync(string topicName, short partitionCount);
}