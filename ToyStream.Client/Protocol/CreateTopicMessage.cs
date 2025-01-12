namespace ToyStream.Client.Protocol;

using Shared;
using ToyStream.Protocol;

public readonly ref struct CreateTopicMessage: IClientMessage
{
    private readonly Utf8String topicName;
    private readonly ShortValue partitionCount;

    public CreateTopicMessage(string topicName, short partitionCount)
    {
        this.topicName = new Utf8String(topicName);
        this.partitionCount = new ShortValue(partitionCount);
    }

    public int Length => this.topicName.Length + partitionCount.Length;
    
    public void Write(ref SpanWriter writer)
    {
        writer.Write(MessageType.CreateTopic);
        this.topicName.Write(ref writer);
        this.partitionCount.Write(ref writer);
    }
}