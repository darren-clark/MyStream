using MyStream.Protocol.Primitives;

namespace MyStream.Protocol.Messages;

public readonly ref struct ConsumerInitRequestMessage: IRequestMessage<ConsumerInitRequestMessage, ConsumerInitResponseMessage>
{
    public required RefUtf8String ConsumerGroup { get; init; }
    public required PrimitiveList<Utf8String, RefUtf8String> Topics { get; init; }
    public required OffsetReset OffsetReset { get; init; }

    public static MessageType Type => MessageType.InitConsumer;

    public static bool TryRead<TReader>(scoped ref TReader reader, out ConsumerInitRequestMessage value, out int bytesToRead) where TReader : IPrimitiveReader, allows ref struct
    {
        if (reader.TryReadPrimitive(out RefUtf8String consumerGroup, out bytesToRead) &&
            reader.TryReadPrimitive(out PrimitiveList<Utf8String, RefUtf8String> topics, out bytesToRead) &&
            reader.TryReadEnum(out OffsetReset offsetReset, out bytesToRead)
           )
        {
            value = new ConsumerInitRequestMessage
            {
                ConsumerGroup = consumerGroup, Topics = topics, OffsetReset = offsetReset,
            };
        }

        value = default;
        return false;
    }

    public void Write<TWriter>(TWriter writer) where TWriter : struct, IPrimitiveWriter, allows ref struct
    {
        writer.Write(ConsumerGroup);
        writer.Write(Topics);
        writer.WriteEnum(OffsetReset);
    }
}

public ref struct ConsumerInitResponseMessage : IResponseMessage<ConsumerInitResponseMessage>
{
    public required ConsumerInitResult Result { get; init; }
    public required PrimitiveList<ConsumerInitPartition> Partitions { get; init; }
    
    public static ConsumerInitResponseMessage InvalidTopic() => new()
    {
        Result = ConsumerInitResult.InvalidTopic, 
        Partitions = default,
    };
    
    public static ConsumerInitResponseMessage Success(Span<ConsumerInitPartition> results) => new()
    {
        Result = ConsumerInitResult.Success,
        Partitions = new PrimitiveList<ConsumerInitPartition>(results),
    };
    
    public static bool TryRead<TReader>(scoped ref TReader reader, out ConsumerInitResponseMessage value, out int bytesToRead) where TReader : IPrimitiveReader, allows ref struct
    {
        if (!reader.TryReadEnum(out ConsumerInitResult result, out bytesToRead))
        {
            value = default;
            return false;
        }
        
        if (result != ConsumerInitResult.Success)
        {
            value = new ConsumerInitResponseMessage { Result = result, Partitions = default};
            return false;
        }

        if (reader.TryReadPrimitive(out PrimitiveList<ConsumerInitPartition> partitions, out bytesToRead))
        {
            value = new ConsumerInitResponseMessage { Result = result, Partitions = partitions };
            return true;
        }

        value = default;
        return false;
    }

    public void Write<TWriter>(TWriter writer) where TWriter : struct, IPrimitiveWriter, allows ref struct
    {
        writer.WriteEnum(Result);
        writer.Write(Partitions);
    }

    public static ResponseType Type => ResponseType.InitConsumer;
}

public enum ConsumerInitResult
{
    Success,
    InvalidTopic
}

public record struct ConsumerInitPartition(short TopicIndex, short Partition, long InitialOffset)
    : IProtocolPrimitive<ConsumerInitPartition>
{
    public static bool TryRead<TReader>(scoped ref TReader reader, out ConsumerInitPartition value, out int bytesToRead) where TReader : IPrimitiveReader, allows ref struct
    {
        if (reader.TryReadInt16(out short topicIndex, out bytesToRead) &&
            reader.TryReadInt16(out short partition, out bytesToRead) &&
            reader.TryReadInt64(out long initialOffset, out bytesToRead))        {
            value = new ConsumerInitPartition(topicIndex, partition, initialOffset);
            return true;
        }
        
        value = default;
        return false;
    }

    public void Write<TWriter>(TWriter writer) where TWriter : struct, IPrimitiveWriter, allows ref struct
    {
        writer.Write(TopicIndex);
        writer.Write(Partition);
        writer.Write(InitialOffset);
    }
}