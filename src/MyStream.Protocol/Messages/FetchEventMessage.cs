using MyStream.Protocol.Primitives;
using MyStream.Protocol.Responses;

namespace MyStream.Protocol.Messages;

public readonly ref struct FetchEventRequestMessage :
    IRequestMessage<FetchEventRequestMessage, FetchEventResponseMessage>
{
    public required TimeSpan Timeout { get; init; }
    public required PrimitiveList<FetchOffset> FetchOffsets { get; init; }

    public static bool TryRead<TReader>(scoped ref TReader reader, out FetchEventRequestMessage value,
        out int bytesToRead) where TReader : IPrimitiveReader, allows ref struct
    {
        if (reader.TryReadTimeSpan(out var timeout, out bytesToRead) &&
            reader.TryReadPrimitive(out PrimitiveList<FetchOffset> fetchOffsets, out bytesToRead))
        {
            value = new FetchEventRequestMessage { Timeout = timeout, FetchOffsets = fetchOffsets, };
            return true;
        }

        value = default;
        return false;
    }

    public void Write<TWriter>(TWriter writer) where TWriter : struct, IPrimitiveWriter, allows ref struct
    {
        writer.Write(Timeout);
        writer.Write(FetchOffsets);
    }

    public static MessageType Type => MessageType.Fetch;
}

public readonly record struct FetchOffset(short TopicIndex, short Partition, long Offset)
    : IProtocolPrimitive<FetchOffset>
{
    public static bool TryRead<TReader>(scoped ref TReader reader, out FetchOffset value, out int bytesToRead)
        where TReader : IPrimitiveReader, allows ref struct
    {
        if (reader.TryReadInt16(out var topicIndex, out bytesToRead) &&
            reader.TryReadInt16(out var partition, out bytesToRead) &&
            reader.TryReadInt64(out var offset, out bytesToRead))
        {
            value = new FetchOffset(topicIndex, partition, offset);
            return true;
        }

        value = default;
        return false;
    }

    public void Write<TWriter>(TWriter writer) where TWriter : struct, IPrimitiveWriter, allows ref struct
    {
        writer.Write(TopicIndex);
        writer.Write(Partition);
        writer.Write(Offset);
    }
}

public enum FetchEventResult
{
    Success,
    InvalidTopic,
    InvalidPartition,
    InvalidOffset
}

public readonly record struct FetchEventResponseMessage(FetchEventResult Result, short EventCount)
    : IResponseMessage<FetchEventResponseMessage>
{
    public static FetchEventResponseMessage Success(short eventCount) => new(FetchEventResult.Success, eventCount);
    public static FetchEventResponseMessage InvalidTopic()  => new(FetchEventResult.InvalidTopic,0);
    public static FetchEventResponseMessage InvalidPartition()  => new(FetchEventResult.InvalidPartition,0);
    public static FetchEventResponseMessage InvalidOffset()  => new(FetchEventResult.InvalidOffset,0);
    
    public static bool TryRead<TReader>(scoped ref TReader reader, out FetchEventResponseMessage value, out int bytesToRead) where TReader : IPrimitiveReader, allows ref struct
    {
        if (!reader.TryReadEnum(out FetchEventResult result, out bytesToRead))
        {
            value = default;
            return false;
        }

        if (result != FetchEventResult.Success)
        {
            value = new FetchEventResponseMessage(result, 0);
            return true;
        }
        if (reader.TryReadInt16(out var eventCount, out bytesToRead))
        {
            value = new FetchEventResponseMessage(FetchEventResult.Success, eventCount);
            return true;
        }

        value = default;
        return false;
    }

    public void Write<TWriter>(TWriter writer) where TWriter : struct, IPrimitiveWriter, allows ref struct
    {
        writer.WriteEnum(Result);
        writer.Write(EventCount);
    }

    public static ResponseType Type => ResponseType.Fetch;
}