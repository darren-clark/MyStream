namespace ToyStream.Topics.InMemory;

using Server;
using Shared;

internal class TopicManager: ITopicManager
{
    private readonly VariantDictionary<Utf8String, Topic> topics = new();
    
    public ValueTask CreateTopicAsync(RefUtf8String topicName, short partitionCount)
    {
        if (this.topics.ContainsKey(topicName))
        {
            
        }
        throw new NotImplementedException();
    }
}