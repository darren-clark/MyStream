using ToyStream.Protocol.Responses;

namespace ToyStream.Protocol.Messages;

using Shared;

public readonly ref struct CreateTopicMessage: IRequestMessage<CreateTopicMessage, AcknowledgeMessage, AcknowledgeResponse>
{
    public required RefUtf8String TopicName { get; init; }
    public required short PartitionCount { get; init; }

    public MessageType Type => MessageType.CreateTopic;

    static bool IMessage<CreateTopicMessage>.TryRead<TReader>(ref TReader reader, out CreateTopicMessage message, out int bytesToRead)
    {
        var tryReader = reader;
        if (!tryReader.TryReadUtf8String(out var topicName, out bytesToRead))
        {
            message = default;
            return false;   
        }
        
        if (!tryReader.TryReadInt16(out var partitionCount, out bytesToRead))
        {
            message = default;
            return false;
        }

        reader = tryReader;
        message = new CreateTopicMessage
        {
            TopicName = topicName,
            PartitionCount = partitionCount
        };
        return true;
    }
    
    internal void Write<TWriter>(TWriter writer) where TWriter: IPrimitiveWriter, allows ref struct
    {
        writer.Write(TopicName);
        writer.Write(PartitionCount);
    }

    void IMessage<CreateTopicMessage>.Write<TWriter>(TWriter writer) => Write(writer);
}