namespace ToyStream.Protocol.Send;

using Shared;

public readonly ref struct CreateTopicMessage: IMessage
{
    private readonly Utf8String topicName;
    private readonly ShortValue partitionCount;

    public CreateTopicMessage(string topicName, short partitionCount)
    {
        this.topicName = new Utf8String(topicName);
        this.partitionCount = new ShortValue(partitionCount);
    }

    public MessageType Type => MessageType.CreateTopic;

    public int Size => this.topicName.Size + this.partitionCount.Size;
    
    public void Write(ref SpanWriter writer)
    {
        this.topicName.Write(ref writer);
        this.partitionCount.Write(ref writer);
    }
}