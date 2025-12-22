namespace ToyStream.Shared;

using System.Buffers;
using System.Text;

/// <summary>
/// Wraps a buffer containing UTF-8 encoded text.
///
/// </summary>
public readonly ref struct RefUtf8String: IReferenceTo<Utf8String>, ICopyableTo<RefUtf8String, Utf8String>
{
    private readonly ReadOnlySequence<byte> value;

    public RefUtf8String(ReadOnlySequence<byte> value)
    {
        this.value = value;
    }

    public RefUtf8String(Utf8String value)
    {
        this.value = new ReadOnlySequence<byte>(value.Memory);
    }
    
    public ReadOnlySequence<byte> Sequence => this.value;
    public short Length => (short) this.value.Length;

    public static explicit operator Utf8String(RefUtf8String value) => new Utf8String(value.value);
    public static explicit operator string(RefUtf8String value) => Encoding.UTF8.GetString(value.value);
    public static implicit operator RefUtf8String(Utf8String value) => new RefUtf8String(value);

    public bool Equals(Utf8String? other)
    {
        if (other == null) return false;
        
        if (other.Memory.Length != this.value.Length)
        {
            return false;
        }

        if (this.value.IsSingleSegment)
        {
            return other.Memory.Span.SequenceEqual(this.value.FirstSpan);
        }

        var reader = new SequenceReader<byte>(this.value);
        var otherSpan = other.Memory.Span;
        while(reader.Remaining > 0)
        {
            var span = reader.CurrentSpan;
            if (!span.SequenceEqual(otherSpan[..span.Length]))
            {
                return false;
            }
            otherSpan = otherSpan[span.Length..];
            reader.Advance(span.Length);
        }

        return true;
    }

    public override int GetHashCode()
    {
        var hashCode = new HashCode();
        // First if it's a single span, use directly.
        if (this.value.IsSingleSegment)
        {
            hashCode.AddBytes(this.value.FirstSpan);
            return hashCode.ToHashCode();
        }
        
        // Second if it's small, then copy it to a span.
        if (this.value.Length <= 128)
        {
            Span<byte> buffer = stackalloc byte[(int) this.value.Length];
            this.value.CopyTo(buffer);
            hashCode.AddBytes(buffer);
            return hashCode.ToHashCode();
        }

        // Copy locally to avoid mutating the original.
        var toHash = this.value;

        // Finally fall back to iterating over the sequence.
        while (!toHash.IsSingleSegment)
        {
            hashCode.AddBytes(toHash.FirstSpan);
            toHash = toHash.Slice(toHash.FirstSpan.Length);
        }
        hashCode.AddBytes(toHash.FirstSpan);
        
        return hashCode.ToHashCode();    
    }
}