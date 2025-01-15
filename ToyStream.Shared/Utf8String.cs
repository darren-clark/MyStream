namespace ToyStream.Shared;

using System.Buffers;
using System.Text;

public class Utf8String: IEquatable<Utf8String>
{
    private readonly Memory<byte> value;

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

    public ReadOnlySpan<byte> Span => this.value.Span;
    
    public static implicit operator RefUtf8String(Utf8String value) => new(new ReadOnlySequence<byte>(value.value));
}