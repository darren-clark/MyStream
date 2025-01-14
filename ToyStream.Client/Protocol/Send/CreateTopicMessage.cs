namespace ToyStream.Protocol.Send;

using Shared;

internal ref struct CreateTopicMessage: IMessage
{
    private readonly Utf8StringValue topicName;
    private readonly ShortValue partitions;
    
    public MessageType Type => MessageType.CreateTopic;

    public CreateTopicMessage(string topicName, short partitionCount)
    {
        this.topicName = new Utf8StringValue(topicName);
        this.partitions = new ShortValue(partitionCount);
        this.Size = this.topicName.Size + this.partitions.Size;
    }

    public int Size { get; }
    
    public void Write(ref SpanWriter writer)
    {
        this.topicName.Write(ref writer);
        this.partitions.Write(ref writer);
    }
}