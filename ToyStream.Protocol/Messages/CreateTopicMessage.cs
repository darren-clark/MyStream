namespace ToyStream.Protocol.Messages;

using System.Buffers;
using Shared;

public readonly ref struct CreateTopicMessage: IMessage<CreateTopicMessage>
{
    public required RefUtf8String TopicName { get; init; }
    public required short PartitionCount { get; init; }

    public int Size => sizeof(short) + sizeof(short) + TopicName.Length;
    public MessageType Type => MessageType.CreateTopic;

    public static bool TryRead(scoped ref SequenceReader<byte> reader, out CreateTopicMessage message, out int bytesToRead)
    {
        if (!reader.TryReadUtf8String(out var topicName, out bytesToRead))
        {
            message = default;
            return false;   
        }
        
        if (!reader.TryReadInt16(out var partitionCount, out bytesToRead))
        {
            message = default;
            return false;
        }

        message = new CreateTopicMessage
        {
            TopicName = topicName,
            PartitionCount = partitionCount
        };
        return true;
    }

    public void Write(ref BigEndianSpanWriter writer)
    {
        writer.Write(TopicName);
        writer.Write(PartitionCount);
        throw new NotImplementedException();
    }
}