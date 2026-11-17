using MyStream.Protocol.Primitives;

namespace MyStream.Protocol.Messages;

public readonly ref struct InitProducerMessage : IRequestMessage<InitProducerMessage, InitProducerResponseMessage>
{
    private InitProducerMessage(PrimitiveList<Utf8String, RefUtf8String> topics)
    {
        this.Topics = topics;
    }

    public PrimitiveList<Utf8String, RefUtf8String> Topics { get; }


    static bool IProtocolPrimitive<InitProducerMessage>.TryRead<TReader>(scoped ref TReader reader,
        out InitProducerMessage message, out int bytesToRead)
    {
        if (!reader.TryReadPrimitive<PrimitiveList<Utf8String, RefUtf8String>>(out var topics, out bytesToRead))
        {
            message = default;
            return false;
        }

        message = new InitProducerMessage(topics);
        return true;
    }

    public void Write<TWriter>(TWriter writer) where TWriter : struct, IPrimitiveWriter, allows ref struct
    {
        writer.Write(Topics);
    }

    public static MessageType Type => MessageType.InitProducer;
}

public readonly ref struct InitProducerResponseMessage : IResponseMessage<InitProducerResponseMessage>
{
    public required InitProducerResult Result { get; init; }
    public required PrimitiveList<OwnedPartition> OwnedPartitions { get; init; }

    public static bool TryRead<TReader>(scoped ref TReader reader, out InitProducerResponseMessage value,
        out int bytesToRead) where TReader : IPrimitiveReader, allows ref struct
    {
        if (!reader.TryReadEnum<InitProducerResult>(out var result, out bytesToRead))
        {
            value = default;
            return false;
        }
    
        if (result != InitProducerResult.Success)
        {
            value = new InitProducerResponseMessage { Result = result, OwnedPartitions = default};
            return true;
        }

        if (reader.TryReadPrimitive(out PrimitiveList<OwnedPartition> ownedPartitions, out bytesToRead))
        {
            value = new InitProducerResponseMessage { Result = result, OwnedPartitions = ownedPartitions };
            return true;
        }

        value = default;
        return false;
    }

    public void Write<TWriter>(TWriter writer) where TWriter : struct, IPrimitiveWriter, allows ref struct
    {
        writer.WriteEnum(Result);
        if (Result == InitProducerResult.Success)
        {
            writer.Write(OwnedPartitions);
        }
    }

    public static ResponseType Type => ResponseType.InitProducer;
    
    public static InitProducerResponseMessage UnknownTopic() => new() { Result = InitProducerResult.UnknownTopic, OwnedPartitions = default };
}

public readonly record struct OwnedPartition(short PartitionIndex, short Partition) : IProtocolPrimitive<OwnedPartition>
{
    public void Write<TWriter>(TWriter writer) where TWriter : struct, IPrimitiveWriter, allows ref struct
    {
        writer.Write(PartitionIndex);
        writer.Write(Partition);
    }

    public static bool TryRead<TReader>(scoped ref TReader reader, out OwnedPartition value, out int bytesToRead)
        where TReader : IPrimitiveReader, allows ref struct
    {
        if (reader.TryReadInt16(out var partitionIndex, out bytesToRead) &&
            reader.TryReadInt16(out var partition, out bytesToRead))
        {
            value = new OwnedPartition(partitionIndex, partition);
            return true;
        }

        value = default;
        return false;
    }
}

public enum InitProducerResult
{
    UnknownTopic,
    Success
}