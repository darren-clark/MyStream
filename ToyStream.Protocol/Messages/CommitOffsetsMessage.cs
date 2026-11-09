namespace ToyStream.Protocol.Messages;

using System.Buffers;
using System.Buffers.Binary;

/// <summary>Where a subscriber has processed up to: the next offset it expects, not the last it saw.</summary>
public readonly record struct CommittedOffset(short PartitionId, long Offset);

/// <summary>
/// A bounded run of fixed size commit records, read as a view over the pipe buffer and written
/// straight from the caller's span. The same bargain <see cref="PartitionDescriptions"/> makes.
/// </summary>
public readonly ref struct CommittedOffsets
{
    internal const int RecordSize = sizeof(short) + sizeof(long);

    /// <summary>Set when writing: the caller already holds the records.</summary>
    private readonly ReadOnlySpan<CommittedOffset> decoded;

    /// <summary>Set when reading: the records are still bytes in the connection's buffer.</summary>
    private readonly ReadOnlySequence<byte> encoded;

    public CommittedOffsets(ReadOnlySpan<CommittedOffset> offsets)
    {
        this.decoded = offsets;
        Count = offsets.Length;
    }

    internal CommittedOffsets(ReadOnlySequence<byte> encoded, int count)
    {
        this.encoded = encoded;
        Count = count;
    }

    public int Count { get; }

    public CommittedOffset this[int index]
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
            var offset = this[i];
            writer.Write(offset.PartitionId);
            writer.Write(offset.Offset);
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

    private static CommittedOffset Decode(ReadOnlySequence<byte> record)
    {
        if (record.IsSingleSegment)
        {
            return Decode(record.FirstSpan);
        }

        Span<byte> buffer = stackalloc byte[RecordSize];
        record.CopyTo(buffer);
        return Decode(buffer);
    }

    private static CommittedOffset Decode(ReadOnlySpan<byte> record) =>
        new(BinaryPrimitives.ReadInt16BigEndian(record),
            BinaryPrimitives.ReadInt64BigEndian(record[sizeof(short)..]));

    public ref struct Enumerator
    {
        private readonly CommittedOffsets offsets;
        private int index;

        internal Enumerator(CommittedOffsets offsets)
        {
            this.offsets = offsets;
            this.index = -1;
        }

        public CommittedOffset Current => this.offsets[this.index];

        public bool MoveNext() => ++this.index < this.offsets.Count;
    }
}

/// <summary>
/// One way, and deliberately unanswered: a lost commit costs redelivery, which a subscriber already
/// has to tolerate, so confirming it would buy nothing and would make the read loop a second writer.
/// </summary>
/// <remarks>
/// Carries no topic or group - the subscribe fixed both for the connection's life, so naming them
/// again would only create a way for them to disagree.
/// </remarks>
public readonly ref struct CommitOffsetsMessage : IRequestMessage<CommitOffsetsMessage>
{
    public required CommittedOffsets Offsets { get; init; }

    static bool IMessage<CommitOffsetsMessage>.TryRead<TReader>(ref TReader reader, out CommitOffsetsMessage message,
        out int bytesToRead)
    {
        var tryReader = reader;
        if (!CommittedOffsets.TryReadRecords(ref tryReader, out var records, out var count, out bytesToRead))
        {
            message = default;
            return false;
        }

        reader = tryReader;
        message = new CommitOffsetsMessage { Offsets = new CommittedOffsets(records, count) };
        return true;
    }

    void IMessage.Write<TWriter>(TWriter writer) => Offsets.Write(writer);

    public static MessageType Type => MessageType.CommitOffsets;
}
