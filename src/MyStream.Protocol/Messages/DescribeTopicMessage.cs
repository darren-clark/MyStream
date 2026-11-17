using MyStream.Protocol.Primitives;

namespace MyStream.Protocol.Messages;

public readonly ref struct DescribeTopicRequestMessage:IRequestMessage<DescribeTopicRequestMessage, DescribeTopicResponseMessage>
{
    public static MessageType Type => MessageType.DescribeTopic;

    public required RefUtf8String TopicName { get; init; }
    
    static bool IProtocolPrimitive<DescribeTopicRequestMessage>.TryRead<TReader>(scoped ref TReader reader, out DescribeTopicRequestMessage message, out int bytesToRead)
    {
        if (!reader.TryReadPrimitive<RefUtf8String>(out var topicName, out bytesToRead))
        {
            message = default;
            return false;
        }
        
        message = new DescribeTopicRequestMessage { TopicName = topicName };
        return true;
    }

    public void Write<TWriter>(TWriter writer) where TWriter : struct, IPrimitiveWriter, allows ref struct
    {
        writer.Write(TopicName);
    }
}

public readonly ref struct DescribeTopicResponseMessage:IResponseMessage<DescribeTopicResponseMessage>
{
    public required DescribeTopicResult Result { get; init; }
    public required PrimitiveList<PartitionDescription> Partitions { get; init; }


    public static ResponseType Type => ResponseType.DescribeTopic;

    static bool IProtocolPrimitive<DescribeTopicResponseMessage>.TryRead<TReader>(scoped ref TReader reader, out DescribeTopicResponseMessage message, out int bytesToRead)
    {
        if (!reader.TryReadEnum<DescribeTopicResult>(out var result, out bytesToRead))
        {
            message = default;
            return false;
        }

        // Mirrors Write: a failure carries no partitions, so no count follows it either.
        if (result != DescribeTopicResult.Success)
        {
            message = new DescribeTopicResponseMessage { Result = result, Partitions = default };
            return true;
        }

        if (reader.TryReadPrimitive<PrimitiveList<PartitionDescription>>(out var partitions, out bytesToRead))
        {
            message = new DescribeTopicResponseMessage { Result = result, Partitions = partitions };
            return true;
        }
        
        message = default;
        return false;
    }

    public void Write<TWriter>(TWriter writer) where TWriter : struct, IPrimitiveWriter, allows ref struct
    {
        writer.WriteEnum(Result);
        if (Result == DescribeTopicResult.Success)
        {
            Partitions.Write(writer);
        }
    }
}

public enum DescribeTopicResult : byte
{
    Success,
    UnknownTopic
}