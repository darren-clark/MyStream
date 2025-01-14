namespace ToyStream.Shared;

using System.Buffers;
using System.Text;

public class Utf8StringValue: IEquatable<Utf8StringValue>
{
    private readonly Memory<byte> value;

    public Utf8StringValue(string value)
    {
        this.value = Encoding.UTF8.GetBytes(value);
    }

    public Utf8StringValue(Span<byte> value)
    {
        this.value = this.value.ToArray();
    }

    public Utf8StringValue(ReadOnlySequence<byte> value)
    {
        this.value = this.value.ToArray();
    }
    
    public bool Equals(Utf8StringValue? other) =>
        other switch
        {
            null => false,
            _ => this.value.Span.SequenceEqual(other.value.Span)
        };

    public override bool Equals(object? obj) =>
        obj switch
        {
            Utf8StringValue utf8String => Equals(utf8String),
            _ => false
        };

    public override int GetHashCode()
    {
        var hashCode = new HashCode();
        hashCode.AddBytes(this.value.Span);
        return hashCode.ToHashCode();
    }

    public ReadOnlySpan<byte> Span => this.value.Span;
}