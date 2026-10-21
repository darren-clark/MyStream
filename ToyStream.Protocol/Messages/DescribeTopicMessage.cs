using ToyStream.Protocol.Responses;
using ToyStream.Shared;

namespace ToyStream.Protocol.Messages;

public readonly ref struct DescribeTopicRequestMessage:IRequestMessage<DescribeTopicRequestMessage, DescribeTopicResponseMessage>
{
    public static MessageType Type => MessageType.DescribeTopic;

    public required RefUtf8String TopicName { get; init; }
    
    static bool IMessage<DescribeTopicRequestMessage>.TryRead<TReader>(ref TReader reader, out DescribeTopicRequestMessage message, out int bytesToRead)
    {
        var tryReader = reader;
        if (!tryReader.TryReadUtf8String(out var topicName, out bytesToRead))
        {
            message = default;
            return false;
        }

        reader = tryReader;
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

    public required PartitionDescriptions Partitions { get; init; }

    public static ResponseType Type => ResponseType.DescribeTopic;

    static bool IMessage<DescribeTopicResponseMessage>.TryRead<TReader>(ref TReader reader, out DescribeTopicResponseMessage message, out int bytesToRead)
    {
        var tryReader = reader;
        if (!tryReader.TryReadEnum<DescribeTopicResult>(out var result, out bytesToRead))
        {
            message = default;
            return false;
        }

        // Mirrors Write: a failure carries no partitions, so no count follows it either.
        if (result != DescribeTopicResult.Success)
        {
            reader = tryReader;
            message = new DescribeTopicResponseMessage { Result = result, Partitions = default };
            return true;
        }

        if (result != DescribeTopicResult.Success)
        {
            message = new DescribeTopicResponseMessage
            {
                Result = result,
                Partitions = default
            };
        }
        
        if (!tryReader.TryReadInt16(out var partitionCount, out bytesToRead))
        {
            message = default;
            return false;
        }

        // Unsigned so a negative count is rejected by the same comparison as an oversized one.
        if ((uint)partitionCount > ProtocolConstants.MaxPartitions)
        {
            ThrowHelpers.ThrowInvalidLength(partitionCount, ProtocolConstants.MaxPartitions);
        }

        if (!tryReader.TryReadExact(partitionCount * PartitionDescriptions.RecordSize, out var records,
                out bytesToRead))
        {
            message = default;
            return false;
        }

        reader = tryReader;
        message = new DescribeTopicResponseMessage
        {
            Result = result,
            Partitions = new PartitionDescriptions(records, partitionCount)
        };
        return true;
    }

    public void Write<TWriter>(TWriter writer) where TWriter : struct, IPrimitiveWriter, allows ref struct
    {
        writer.Write(Result);
        if (Result == DescribeTopicResult.Success)
        {
            Partitions.Write(writer);
        }
    }
}