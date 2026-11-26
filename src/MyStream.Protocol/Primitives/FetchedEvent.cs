using System.IO.Pipelines;

namespace MyStream.Protocol.Primitives;

public readonly record struct FetchedEventHeader(short TopicIndex, short Partition, long Offset)
    : IProtocolPrimitive<FetchedEventHeader>
{
    public static bool TryRead<TReader>(scoped ref TReader reader, out FetchedEventHeader value,
        out int bytesToRead) where TReader : IPrimitiveReader, allows ref struct
    {
        if (reader.TryReadInt16(out var topicIndex, out bytesToRead) &&
            reader.TryReadInt16(out var partition, out bytesToRead) &&
            reader.TryReadInt64(out var offset, out bytesToRead))
        {
            value = new FetchedEventHeader(topicIndex, partition, offset);
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