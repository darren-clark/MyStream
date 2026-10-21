using System.Buffers;
using ToyStream.Shared;

namespace ToyStream.Protocol.Messages;

/// <summary>
/// The topic names a fetch refers to, written once and referenced by index. Names are variable
/// length so they are scanned rather than indexed, which is cheap at this bound.
/// </summary>
public readonly ref struct TopicNames
{
    /// <summary>Set when writing from a caller's list. Utf8String rather than string so the names
    /// are already encoded, and stable enough to outlive the buffer a request arrived in.</summary>
    private readonly ReadOnlySpan<Utf8String> names;

    /// <summary>Set when reading, or when echoing a table straight back.</summary>
    private readonly ReadOnlySequence<byte> encoded;

    public TopicNames(ReadOnlySpan<Utf8String> names)
    {
        this.names = names;
        Count = names.Length;
    }

    private TopicNames(ReadOnlySequence<byte> encoded, int count)
    {
        this.encoded = encoded;
        Count = count;
    }

    public int Count { get; }

    public RefUtf8String this[int index]
    {
        get
        {
            ArgumentOutOfRangeException.ThrowIfGreaterThanOrEqual((uint)index, (uint)Count);

            if (this.encoded.IsEmpty)
            {
                return this.names[index];
            }

            var reader = new PrimitiveReader(this.encoded);
            RefUtf8String name = default;
            for (var i = 0; i <= index; i++)
            {
                reader.TryReadUtf8String(out name, out _);
            }

            return name;
        }
    }

    internal void Write<TWriter>(TWriter writer) where TWriter : struct, IPrimitiveWriter, allows ref struct
    {
        writer.Write((short)Count);
        for (var i = 0; i < Count; i++)
        {
            writer.Write(this[i]);
        }
    }

    internal static bool TryRead<TReader>(scoped ref TReader reader, out TopicNames value, out int bytesToRead)
        where TReader : IPrimitiveReader, allows ref struct
    {
        var tryReader = reader;
        if (!tryReader.TryReadInt16(out var count, out bytesToRead))
        {
            value = default;
            return false;
        }

        // Unsigned so a negative count is rejected by the same comparison as an oversized one.
        if ((uint)count > ProtocolConstants.MaxTopicsPerFetch)
        {
            ThrowHelpers.ThrowInvalidLength(count, ProtocolConstants.MaxTopicsPerFetch);
        }

        // Scanned to find the table's extent, then kept as a view over it.
        var start = tryReader.Remaining;
        for (var i = 0; i < count; i++)
        {
            if (!tryReader.TryReadUtf8String(out _, out bytesToRead))
            {
                value = default;
                return false;
            }
        }

        reader = tryReader;
        value = new TopicNames(start.Slice(0, start.Length - tryReader.Remaining.Length), count);
        bytesToRead = 0;
        return true;
    }
}
