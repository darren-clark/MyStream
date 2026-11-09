namespace ToyStream.Protocol.Messages;

using System.Buffers;
using System.Buffers.Binary;

/// <summary>A bounded run of partition ids, read as a view over the pipe buffer.</summary>
public readonly ref struct PartitionIds
{
    internal const int RecordSize = sizeof(short);

    private readonly ReadOnlySpan<short> decoded;
    private readonly ReadOnlySequence<byte> encoded;

    public PartitionIds(ReadOnlySpan<short> ids)
    {
        this.decoded = ids;
        Count = ids.Length;
    }

    internal PartitionIds(ReadOnlySequence<byte> encoded, int count)
    {
        this.encoded = encoded;
        Count = count;
    }

    public int Count { get; }

    public short this[int index]
    {
        get
        {
            ArgumentOutOfRangeException.ThrowIfGreaterThanOrEqual((uint)index, (uint)Count);

            if (this.encoded.IsEmpty)
            {
                return this.decoded[index];
            }

            var record = this.encoded.Slice(index * RecordSize, RecordSize);
            if (record.IsSingleSegment)
            {
                return BinaryPrimitives.ReadInt16BigEndian(record.FirstSpan);
            }

            Span<byte> buffer = stackalloc byte[RecordSize];
            record.CopyTo(buffer);
            return BinaryPrimitives.ReadInt16BigEndian(buffer);
        }
    }

    public Enumerator GetEnumerator() => new(this);

    internal void Write<TWriter>(TWriter writer) where TWriter : struct, IPrimitiveWriter, allows ref struct
    {
        writer.Write((short)Count);
        for (var i = 0; i < Count; i++)
        {
            writer.Write(this[i]);
        }
    }

    /// <summary>
    /// Hands back the encoded run rather than the view over it: an <c>out</c> ref struct is scoped
    /// to the reader it came from, so it cannot be built here and returned.
    /// </summary>
    internal static bool TryReadRecords<TReader>(ref TReader reader, out ReadOnlySequence<byte> records,
        out short count, out int bytesToRead)
        where TReader : IPrimitiveReader, allows ref struct
    {
        if (!reader.TryReadInt16(out count, out bytesToRead))
        {
            records = default;
            return false;
        }

        // Unsigned so a negative count is rejected by the same comparison as an oversized one.
        if ((uint)count > ProtocolConstants.MaxPartitions)
        {
            ThrowHelpers.ThrowInvalidLength(count, ProtocolConstants.MaxPartitions);
        }

        return reader.TryReadExact(count * RecordSize, out records, out bytesToRead);
    }

    public ref struct Enumerator
    {
        private readonly PartitionIds ids;
        private int index;

        internal Enumerator(PartitionIds ids)
        {
            this.ids = ids;
            this.index = -1;
        }

        public short Current => this.ids[this.index];

        public bool MoveNext() => ++this.index < this.ids.Count;
    }
}

/// <summary>
/// Takes partitions back from a subscriber. Written where the pump stops pushing them, so its place
/// in the stream is what fixes the final offset: everything for these partitions precedes it.
/// </summary>
/// <remarks>
/// One way rather than a request, because the send slot is single-occupancy and awaiting an answer
/// would stop the pump pushing anything else until the subscriber reached it.
/// </remarks>
public readonly ref struct RevokeMessage : IRequestMessage<RevokeMessage>
{
    /// <summary>Distinguishes an answer to this revoke from one to an earlier, already timed out.</summary>
    public required int Epoch { get; init; }

    public required PartitionIds Partitions { get; init; }

    static bool IMessage<RevokeMessage>.TryRead<TReader>(ref TReader reader, out RevokeMessage message,
        out int bytesToRead)
    {
        var tryReader = reader;
        if (!tryReader.TryReadInt32(out var epoch, out bytesToRead) ||
            !PartitionIds.TryReadRecords(ref tryReader, out var records, out var count, out bytesToRead))
        {
            message = default;
            return false;
        }

        reader = tryReader;
        message = new RevokeMessage { Epoch = epoch, Partitions = new PartitionIds(records, count) };
        return true;
    }

    void IMessage.Write<TWriter>(TWriter writer)
    {
        writer.Write(this.Epoch);
        this.Partitions.Write(writer);
    }

    public static MessageType Type => MessageType.Revoke;
}

/// <summary>
/// Answers a <see cref="RevokeMessage"/> with where the subscriber stopped. Reports what it has
/// committed rather than what it was handed, so a subscriber deferring its work past the cursor
/// cannot hand on a position it never reached.
/// </summary>
public readonly ref struct RevokeCompleteMessage : IRequestMessage<RevokeCompleteMessage>
{
    public required int Epoch { get; init; }

    public required CommittedOffsets Offsets { get; init; }

    static bool IMessage<RevokeCompleteMessage>.TryRead<TReader>(ref TReader reader,
        out RevokeCompleteMessage message, out int bytesToRead)
    {
        var tryReader = reader;
        if (!tryReader.TryReadInt32(out var epoch, out bytesToRead) ||
            !CommittedOffsets.TryReadRecords(ref tryReader, out var records, out var count, out bytesToRead))
        {
            message = default;
            return false;
        }

        reader = tryReader;
        message = new RevokeCompleteMessage { Epoch = epoch, Offsets = new CommittedOffsets(records, count) };
        return true;
    }

    void IMessage.Write<TWriter>(TWriter writer)
    {
        writer.Write(this.Epoch);
        this.Offsets.Write(writer);
    }

    public static MessageType Type => MessageType.RevokeComplete;
}
