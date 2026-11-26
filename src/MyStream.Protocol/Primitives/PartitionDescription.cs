namespace MyStream.Protocol.Primitives;

/// <summary>
/// Offsets are a half open range, so an empty partition is simply Start == End. Shared rather than
/// protocol owned: it is storage data that happens to also go on the wire.
/// </summary>
public readonly record struct PartitionDescription(short Partition, long StartOffset, long EndOffset)
    : IProtocolPrimitive<PartitionDescription>
{
    public void Write<TWriter>(TWriter writer) where TWriter : struct, IPrimitiveWriter, allows ref struct
    {
        writer.Write(Partition);
        writer.Write(StartOffset);
        writer.Write(EndOffset);
    }

    public static bool TryRead<TReader>(scoped ref TReader reader, out PartitionDescription value, out int bytesToRead)
        where TReader : IPrimitiveReader, allows ref struct
    {
        if (reader.TryReadInt16(out short partition, out bytesToRead) &&
            reader.TryReadInt64(out long startOffset, out bytesToRead) &&
            reader.TryReadInt64(out long endOffset, out bytesToRead))
        {
            value = new PartitionDescription(partition, startOffset, endOffset);
            return true;
        }

        value = default;
        return false;
    }

    public bool IsValidOffset(long offset) => offset >= StartOffset && offset <= EndOffset; 

}