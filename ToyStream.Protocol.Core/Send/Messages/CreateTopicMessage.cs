namespace ToyStream.Protocol.Send;

using Messages.Send;
using Shared;

public readonly ref struct CreateTopicMessage: IMessage
{
    private readonly Utf8StringValue topicName;
    private readonly ShortValue partitionCount;

    public CreateTopicMessage(string topicName, short partitionCount)
    {
        this.topicName = new Utf8StringValue(topicName);
        this.partitionCount = new ShortValue(partitionCount);
    }

    public MessageType Type => MessageType.CreateTopic;

    public int Size => this.topicName.Size + this.partitionCount.Size;
    
    public void Write(ref ProtocolPrimitiveWriter primitiveWriter)
    {
        this.topicName.Write(ref primitiveWriter);
        this.partitionCount.Write(ref primitiveWriter);
    }
}