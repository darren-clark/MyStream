namespace ToyStream.Shared;

using System.Buffers;
using System.Runtime.InteropServices;
using System.Text;

/// <summary>
/// A UTF-8 string in one of three forms: contiguous UTF-8, split UTF-8 from a pipe, or still-UTF-16
/// source text. The last defers transcoding to the write, so a string needs no intermediate buffer.
/// </summary>
public readonly ref struct RefUtf8String: IEquatable<Utf8String>
{
    /// <summary>Above this, flattening for a hash or a compare goes to the heap rather than the stack.</summary>
    private const int MaxStackAllocBytes = 256;

    /// <summary>Contiguous data, so the source may be stack memory. Holds UTF-16 when <see cref="isUtf16"/>.</summary>
    private readonly ReadOnlySpan<byte> span;

    /// <summary>Set only when the data is genuinely split, which a span cannot represent.</summary>
    private readonly ReadOnlySequence<byte> sequence;

    private readonly bool isUtf16;

    public RefUtf8String(ReadOnlySequence<byte> value)
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

    public RefUtf8String(ReadOnlySpan<byte> value)
    {
        this.span = value;
    }

    public RefUtf8String(ReadOnlySpan<char> value)
    {
        this.span = MemoryMarshal.AsBytes(value);
        this.isUtf16 = true;
    }

    public RefUtf8String(Utf8String value)
    {
        this.span = value.Memory.Span;
    }

    private ReadOnlySpan<char> Chars => MemoryMarshal.Cast<byte, char>(this.span);

    public short Length => (short) GetByteCount();

    /// <summary>The encoded length, which for UTF-16 source is not the span length.</summary>
    public int GetByteCount() =>
        this.isUtf16 ? Encoding.UTF8.GetByteCount(Chars)
        : this.sequence.IsEmpty ? this.span.Length
        : (int) this.sequence.Length;

    /// <summary>
    /// The single way out of all three forms, so no caller can emit UTF-16 bytes by failing to check
    /// which form it holds. Transcodes straight into <paramref name="destination"/>.
    /// </summary>
    public int CopyTo(Span<byte> destination)
    {
        if (this.isUtf16)
        {
            return Encoding.UTF8.GetBytes(Chars, destination);
        }

        if (this.sequence.IsEmpty)
        {
            this.span.CopyTo(destination);
            return this.span.Length;
        }

        this.sequence.CopyTo(destination);
        return (int) this.sequence.Length;
    }

    public static explicit operator Utf8String(RefUtf8String value) =>
        value.isUtf16 ? new Utf8String(value.Chars.ToString())
        : value.sequence.IsEmpty ? new Utf8String(value.span)
        : new Utf8String(value.sequence);

    public static explicit operator string(RefUtf8String value) =>
        value.isUtf16 ? value.Chars.ToString()
        : value.sequence.IsEmpty ? Encoding.UTF8.GetString(value.span)
        : Encoding.UTF8.GetString(value.sequence);

    public static implicit operator RefUtf8String(Utf8String value) => new RefUtf8String(value);
    public static implicit operator RefUtf8String(string value) => new RefUtf8String(value.AsSpan());

    public bool Equals(Utf8String? other)
    {
        if (other == null) return false;

        var byteCount = GetByteCount();
        if (other.Memory.Length != byteCount)
        {
            return false;
        }

        var otherSpan = other.Memory.Span;

        if (!this.isUtf16)
        {
            if (this.sequence.IsEmpty)
            {
                return otherSpan.SequenceEqual(this.span);
            }

            var reader = new SequenceReader<byte>(this.sequence);
            while (reader.Remaining > 0)
            {
                var segment = reader.CurrentSpan;
                if (!segment.SequenceEqual(otherSpan[..segment.Length]))
                {
                    return false;
                }

                reader.Advance(segment.Length);
                otherSpan = otherSpan[segment.Length..];
            }

            return true;
        }

        Span<byte> buffer = byteCount <= MaxStackAllocBytes
            ? stackalloc byte[MaxStackAllocBytes]
            : new byte[byteCount];

        return otherSpan.SequenceEqual(buffer[..CopyTo(buffer)]);
    }

    public override int GetHashCode()
    {
        var hashCode = new HashCode();

        // Contiguous UTF-8 can be hashed where it lies.
        if (!this.isUtf16 && this.sequence.IsEmpty)
        {
            hashCode.AddBytes(this.span);
            return hashCode.ToHashCode();
        }

        // Everything else is flattened first: AddBytes consumes whole blocks and holds the remainder,
        // so feeding it segment by segment would hash one name differently per pipe split.
        var byteCount = GetByteCount();
        Span<byte> buffer = byteCount <= MaxStackAllocBytes
            ? stackalloc byte[MaxStackAllocBytes]
            : new byte[byteCount];

        hashCode.AddBytes(buffer[..CopyTo(buffer)]);
        return hashCode.ToHashCode();
    }
}