namespace ToyStream.Shared;

using System.Buffers;
using System.Text;

public class Utf8String: IReferenceTo<Utf8String>
{
    private readonly ReadOnlyMemory<byte> value;

    public Utf8String(string value)
    {
        this.value = Encoding.UTF8.GetBytes(value);
    }

    public Utf8String(ReadOnlySpan<byte> value)
    {
        this.value = this.value.ToArray();
    }

    public Utf8String(ReadOnlySequence<byte> value)
    {
        this.value = this.value.ToArray();
    }
    
    public bool Equals(Utf8String? other) =>
        other switch
        {
            null => false,
            _ => this.value.Span.SequenceEqual(other.value.Span)
        };

    public override bool Equals(object? obj) =>
        obj switch
        {
            Utf8String utf8String => Equals(utf8String),
            _ => false
        };

    public override int GetHashCode()
    {
        var hashCode = new HashCode();
        hashCode.AddBytes(this.value.Span);
        return hashCode.ToHashCode();
    }

    public ReadOnlyMemory<byte> Memory => this.value;
    public static explicit operator Utf8String(string value) => new(value);
    public static explicit operator string (Utf8String value) => Encoding.UTF8.GetString(value.value.Span);
}