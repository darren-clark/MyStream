using System.Buffers;
using System.Runtime.InteropServices;
using System.Text;

using static MyStream.ThrowHelpers;

namespace MyStream.Protocol.Primitives;

/// <summary>
/// A UTF-8 string in one of three forms: contiguous UTF-8, split UTF-8 from a pipe, or still-UTF-16
/// source text. The last defers transcoding to the write, so a string needs no intermediate buffer.
/// </summary>
public readonly ref struct RefUtf8String: IEquatable<Utf8String>, IProtocolPrimitive<RefUtf8String>
{
    private enum StoredForm: byte
    {
        Utf8Span, // Default
        Utf16Span,
        Utf8Sequence
    }

    private readonly StoredForm storedForm;
    
    /// <summary>Above this, flattening for a hash or a compare goes to the heap rather than the stack.</summary>
    private const int MaxStackAllocBytes = 256;

    /// <summary>Contiguous data, so the source may be stack memory. Holds UTF-16 when <see cref="storedForm"/> is <see cref="StoredForm.Utf16Span"/>.</summary>
    private readonly ReadOnlySpan<byte> span;

    /// <summary>Set only when the data is genuinely split, which a span cannot represent.</summary>
    private readonly ReadOnlySequence<byte> sequence;

    public RefUtf8String(ReadOnlySequence<byte> value)
    {
        if (value.IsSingleSegment)
        {
            this.span = value.FirstSpan;
        }
        else
        {
            this.sequence = value;
            this.storedForm = StoredForm.Utf8Sequence;
        }
    }

    public RefUtf8String(ReadOnlySpan<byte> value)
    {
        this.span = value;
    }

    public RefUtf8String(string value)
    {
        this.span = MemoryMarshal.AsBytes(value.AsSpan());
        this.storedForm = StoredForm.Utf16Span;
    }

    public RefUtf8String(Utf8String value)
    {
        this.span = value.Memory.Span;
    }

    // Only valid for utf16 buffer.
    private ReadOnlySpan<char> Chars => MemoryMarshal.Cast<byte, char>(this.span);

    public short Length => (short) GetByteCount();
    
    /// <summary>The encoded length, which for UTF-16 source is not the span length.</summary>
    public int GetByteCount() => storedForm switch
        {
            StoredForm.Utf8Span => this.span.Length,
            StoredForm.Utf8Sequence => (int) this.sequence.Length,
            StoredForm.Utf16Span => Encoding.UTF8.GetByteCount(Chars),
            _ => ThrowInvalidOperation<int>("Invalid stored form")
        };  

    /// <summary>
    /// The single way out of all three forms, so no caller can emit UTF-16 bytes by failing to check
    /// which form it holds. Destination should be allocated to be GetByteCount size.
    /// </summary>
    public int CopyTo(Span<byte> destination)
    {
        switch (storedForm)
        {
            case StoredForm.Utf8Span:
                this.span.CopyTo(destination);
                return this.span.Length;
            case StoredForm.Utf8Sequence:
                this.sequence.CopyTo(destination);
                return (int) this.sequence.Length;
            case StoredForm.Utf16Span:
                return Encoding.UTF8.GetBytes(Chars, destination);
            default:
                ThrowInvalidOperation<int>("Invalid stored form");
                return default;
        }
    }
    
    // Allocates on heap, so is explicit
    public static explicit operator Utf8String(RefUtf8String value) =>
        value.storedForm switch
        {
            StoredForm.Utf8Span => new Utf8String(value.span),
            StoredForm.Utf8Sequence =>new Utf8String(value.sequence),
            StoredForm.Utf16Span => new Utf8String(value.Chars.ToString()),
            _ => ThrowInvalidOperation<Utf8String>("Invalid stored form")
        };

    // Allocates on heap, so is explicit
    public static explicit operator string(RefUtf8String value) =>
        value.storedForm switch
        {
            StoredForm.Utf8Span => Encoding.UTF8.GetString(value.span),
            StoredForm.Utf8Sequence => Encoding.UTF8.GetString(value.sequence),
            StoredForm.Utf16Span => value.Chars.ToString(),
            _ => ThrowInvalidOperation<string>("Invalid stored form")
        };

    // Wraps string buffer, so implicit
    public static implicit operator RefUtf8String(string value) => new(value);
    
    public bool Equals(Utf8String other)
    {
        var byteCount = GetByteCount();
        if (other.Memory.Length != byteCount)
        {
            return false;
        }

        var otherSpan = other.Memory.Span;

        switch (storedForm)
        {
            case StoredForm.Utf8Span:
                return otherSpan.SequenceEqual(this.span);
            // Compare segment by segment.
            case StoredForm.Utf8Sequence:
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
            // Write utf8 to buffer and compare
            case StoredForm.Utf16Span:
                Span<byte> buffer = byteCount <= MaxStackAllocBytes
                    ? stackalloc byte[MaxStackAllocBytes]
                    : new byte[byteCount];

                this.CopyTo(buffer);
                return otherSpan.SequenceEqual(buffer);
            default:
                ThrowInvalidOperation("Invalid stored form");
                return false;
        }
    }

    
    public override int GetHashCode()
    {
        // We'll need to write to temp buffer if it's either a utf-16 string
        // or in a sequence.
        // The reason for the sequence is that multiple calls
        // to AddBytes with additional values doesn't return the 
        // same hash as a single call, so we can't loop over segments.

        var hashCode = new HashCode();
        switch (storedForm)
        {
            case StoredForm.Utf8Span:
                hashCode.AddBytes(this.span);
                break;
            case StoredForm.Utf8Sequence:
            case StoredForm.Utf16Span:
                var byteCount = this.GetByteCount();
                Span<byte> buffer = byteCount <= MaxStackAllocBytes
                    ? stackalloc byte[MaxStackAllocBytes]
                    : new byte[byteCount];
                this.CopyTo(buffer);
                hashCode.AddBytes(buffer);
                break;
            default:
                ThrowInvalidOperation("Invalid stored form");
                break;
        }
        return hashCode.ToHashCode();
    }


    public void Write<TWriter>(TWriter writer) where TWriter : struct, IPrimitiveWriter, allows ref struct
    {
        throw new NotImplementedException();
    }

    public static bool TryRead<TReader>(scoped ref TReader reader, out RefUtf8String message, out int bytesToRead) where TReader : IPrimitiveReader, allows ref struct
    {
        if (!reader.TryReadInt16(out var length, out bytesToRead))
        {
            message = default;
            return false;
        }

        if (!reader.TryReadBytes(length, out var buffer, out bytesToRead))
        {
            message = default;
            return false;
        }
        
        message = new RefUtf8String(buffer);
        return true;
    }
}