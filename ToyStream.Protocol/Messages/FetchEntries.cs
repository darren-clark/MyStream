using System.Buffers;
using System.Buffers.Binary;
using ToyStream.Shared;

namespace ToyStream.Protocol.Messages;

/// <summary>
/// The partitions a fetch asks for. Fixed stride, so reading views the buffer and decodes on
/// demand exactly like a describe's partition list.
/// </summary>
public readonly ref struct FetchEntries
{
    internal const int RecordSize = sizeof(short) + sizeof(short) + sizeof(long);

    private readonly ReadOnlySpan<FetchEntry> decoded;
    private readonly ReadOnlySequence<byte> encoded;

    public FetchEntries(ReadOnlySpan<FetchEntry> entries)
    {
        this.decoded = entries;
        Count = entries.Length;
    }

    private FetchEntries(ReadOnlySequence<byte> encoded, int count)
    {
        this.encoded = encoded;
        Count = count;
    }

    public int Count { get; }

    public FetchEntry this[int index]
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

    internal void Write<TWriter>(TWriter writer) where TWriter : struct, IPrimitiveWriter, allows ref struct
    {
        writer.Write((short)Count);
        for (var i = 0; i < Count; i++)
        {
            var entry = this[i];
            writer.Write(entry.TopicIndex);
            writer.Write(entry.PartitionId);
            writer.Write(entry.Offset);
        }
    }

    internal static bool TryRead<TReader>(scoped ref TReader reader, out FetchEntries value, out int bytesToRead)
        where TReader : IPrimitiveReader, allows ref struct
    {
        var tryReader = reader;
        if (!tryReader.TryReadInt16(out var count, out bytesToRead))
        {
            value = default;
            return false;
        }

        // Unsigned so a negative count is rejected by the same comparison as an oversized one.
        if ((uint)count > ProtocolConstants.MaxFetchEntries)
        {
            ThrowHelpers.ThrowInvalidLength(count, ProtocolConstants.MaxFetchEntries);
        }

        if (!tryReader.TryReadExact(count * RecordSize, out var records, out bytesToRead))
        {
            value = default;
            return false;
        }

        reader = tryReader;
        value = new FetchEntries(records, count);
        return true;
    }

    private static FetchEntry Decode(ReadOnlySequence<byte> record)
    {
        if (record.IsSingleSegment)
        {
            return Decode(record.FirstSpan);
        }

        Span<byte> buffer = stackalloc byte[RecordSize];
        record.CopyTo(buffer);
        return Decode(buffer);
    }

    private static FetchEntry Decode(ReadOnlySpan<byte> record) =>
        new(BinaryPrimitives.ReadInt16BigEndian(record),
            BinaryPrimitives.ReadInt16BigEndian(record[sizeof(short)..]),
            BinaryPrimitives.ReadInt64BigEndian(record[(sizeof(short) * 2)..]));

    public ref struct Enumerator
    {
        private readonly FetchEntries entries;
        private int index;

        internal Enumerator(FetchEntries entries)
        {
            this.entries = entries;
            this.index = -1;
        }

        public FetchEntry Current => this.entries[this.index];

        public bool MoveNext() => ++this.index < this.entries.Count;
    }
}
