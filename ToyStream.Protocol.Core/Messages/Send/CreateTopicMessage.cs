namespace ToyStream.Protocol.Messages.Send;

using Shared;

internal ref struct CreateTopicMessage: IMessage
{
    private readonly Utf8String topicName;
    private readonly short partitionCount;
    
    public MessageType Type => MessageType.CreateTopic;

    public CreateTopicMessage(Utf8String topicName, short partitionCount)
    {
        this.topicName = new RefUtf8String(topicName);
        this.partitionCount = partitionCount;
    }

    public int Size { get; }
    
    public void Write(ref ProtocolPrimitiveWriter primitiveWriter)
    {
        primitiveWriter.Write(this.topicName);
        primitiveWriter.Write(this.partitionCount);
    }
}