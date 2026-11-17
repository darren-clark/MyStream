using System.Buffers;

using MyStream.Protocol.Primitives;

namespace MyStream;

/// <summary>
/// A bounded run of opaque bytes read from, or written to, the wire. Span-backed when contiguous so
/// a caller may pass stack memory, and sequence-backed only when the pipe split it.
/// </summary>
public readonly ref struct RefBytes: IProtocolPrimitive<RefBytes>
{
    private readonly ReadOnlySpan<byte> span;
    private readonly ReadOnlySequence<byte> sequence;

    public RefBytes(ReadOnlySequence<byte> value)
    {
        if (value.IsSingleSegment)
        {
            this.span = value.FirstSpan;
        }
        else
        {
            this.sequence = value;
        }
    }

    public RefBytes(ReadOnlySpan<byte> value)
    {
        this.span = value;
    }

    public bool IsEmpty => Length == 0;

    public int Length => this.sequence.IsEmpty ? this.span.Length : (int)this.sequence.Length;

    /// <summary>The single way out of both forms, so a caller cannot read the wrong one.</summary>
    public int CopyTo(Span<byte> destination)
    {
        if (this.sequence.IsEmpty)
        {
            this.span.CopyTo(destination);
            return this.span.Length;
        }

        this.sequence.CopyTo(destination);
        return (int)this.sequence.Length;
    }

    public byte[] ToArray() => this.sequence.IsEmpty ? [.. this.span] : this.sequence.ToArray();

    public static implicit operator RefBytes(ReadOnlySpan<byte> value) => new(value);

    public static implicit operator RefBytes(byte[]? value) => new(value.AsSpan());
    public void Write<TWriter>(TWriter writer) where TWriter : struct, IPrimitiveWriter, allows ref struct
    {
        if (this.sequence.IsEmpty)
        {
            writer.Write((short)this.span.Length);
            writer.Write(this.span);
        }
        else
        {
            writer.Write((short)this.sequence.Length);
            writer.Write(this.sequence);
        }
    }

    public static bool TryRead<TReader>(scoped ref TReader reader, out RefBytes value, out int bytesToRead) where TReader : IPrimitiveReader, allows ref struct
    {
        if (!reader.TryReadInt16(out var length, out bytesToRead))
        {
            value = default;
            return false;
        }

        if (!reader.TryReadExact(length, out var buffer, out bytesToRead))
        {
            value = default;
            return false;
        }

        value = new RefBytes(buffer);
        return true;
    }
}
