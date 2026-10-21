namespace ToyStream.Protocol.Messages;

using System.Buffers;
using System.Buffers.Binary;
using ToyStream.Shared;

/// <summary>
/// A bounded run of fixed size partition records. Reading views the pipe buffer and decodes on
/// demand; writing points at the caller's span. Neither side materialises a list.
/// </summary>
public readonly ref struct PartitionDescriptions
{
    internal const int RecordSize = sizeof(long) + sizeof(long);

    /// <summary>Set when writing: the caller already holds the records.</summary>
    private readonly ReadOnlySpan<PartitionDescription> decoded;

    /// <summary>Set when reading: the records are still bytes in the connection's buffer.</summary>
    private readonly ReadOnlySequence<byte> encoded;

    public PartitionDescriptions(ReadOnlySpan<PartitionDescription> descriptions)
    {
        this.decoded = descriptions;
        Count = descriptions.Length;
    }

    internal PartitionDescriptions(ReadOnlySequence<byte> encoded, int count)
    {
        this.encoded = encoded;
        Count = count;
    }

    public int Count { get; }

    public PartitionDescription this[int index]
    {
        get
        {
            ArgumentOutOfRangeException.ThrowIfGreaterThanOrEqual((uint)index, (uint)Count);

            return this.encoded.IsEmpty
                ? this.decoded[index]
                : Decode(this.encoded.Slice(index * RecordSize, RecordSize));
        }
    }

    public Enumerator GetEnumerator() => new(this);

    public int CopyTo(Span<PartitionDescription> destination)
    {
        for (var i = 0; i < Count; i++)
        {
            destination[i] = this[i];
        }

        return Count;
    }

    internal void Write<TWriter>(TWriter writer) where TWriter : struct, IPrimitiveWriter, allows ref struct
    {
        writer.Write((short)Count);
        for (var i = 0; i < Count; i++)
        {
            var description = this[i];
            writer.Write(description.StartOffset);
            writer.Write(description.EndOffset);
        }
    }

    private static PartitionDescription Decode(ReadOnlySequence<byte> record)
    {
        if (record.IsSingleSegment)
        {
            return Decode(record.FirstSpan);
        }

        Span<byte> buffer = stackalloc byte[RecordSize];
        record.CopyTo(buffer);
        return Decode(buffer);
    }

    private static PartitionDescription Decode(ReadOnlySpan<byte> record) =>
        new(BinaryPrimitives.ReadInt64BigEndian(record),
            BinaryPrimitives.ReadInt64BigEndian(record[sizeof(long)..]));

    public ref struct Enumerator
    {
        private readonly PartitionDescriptions descriptions;
        private int index;

        internal Enumerator(PartitionDescriptions descriptions)
        {
            this.descriptions = descriptions;
            this.index = -1;
        }

        public PartitionDescription Current => this.descriptions[this.index];

        public bool MoveNext() => ++this.index < this.descriptions.Count;
    }
}
