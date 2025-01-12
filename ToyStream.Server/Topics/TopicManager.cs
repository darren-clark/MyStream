namespace ToyStream.Server.Topics;

using System.Runtime.CompilerServices;

public interface ITopicManager
{
    ValueTask AddTopicAsync(string topicName, short partitionCount);
}

public class TopicManager: ITopicManager
{
    private readonly Dictionary<string, Topic> topics = new();
    
    public async ValueTask AddTopicAsync(string topicName, short partitionCount)
    {
        this.topics.Add(topicName, new Topic(partitionCount));
    }


    private class Topic
    {
        private readonly TopicPartition[] partitions;

        public Topic(short partitionCount)
        {
            this.partitions = new TopicPartition[partitionCount];
        }
    }
    
    private class TopicPartition
    {
            
    }
}