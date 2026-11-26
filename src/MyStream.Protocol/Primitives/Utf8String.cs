using System.Buffers;
using System.Text;

namespace MyStream.Protocol.Primitives;

public readonly struct Utf8String: IEquatable<Utf8String>, IRefConvertible<Utf8String, RefUtf8String>
{
    private readonly ReadOnlyMemory<byte> value;

    public Utf8String(string value)
    {
        this.value = Encoding.UTF8.GetBytes(value);
    }

    public Utf8String(ReadOnlySpan<byte> value)
    {
        this.value = value.ToArray();
    }

    public Utf8String(ReadOnlySequence<byte> value)
    {
        this.value = value.ToArray();
    }
    
    public bool Equals(Utf8String other) =>
        this.value.Span.SequenceEqual(other.value.Span);

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

    public static Utf8String Empty { get; } = new(string.Empty);

    /// <summary>Ordinal over the encoded bytes, which is the same order every node will compute.</summary>
    public int CompareTo(Utf8String other) => this.value.Span.SequenceCompareTo(other.value.Span);

    public static explicit operator Utf8String(string value) => new(value);
    public static explicit operator string (Utf8String value) => Encoding.UTF8.GetString(value.value.Span);
    
    public static implicit operator RefUtf8String(Utf8String value) => new(value);
    
    public static bool TryRead<TReader>(scoped ref TReader reader, out Utf8String value, out int bytesToRead) where TReader : IPrimitiveReader, allows ref struct
    {
        throw new NotImplementedException();
    }
    
    public void Write<TWriter>(TWriter writer) where TWriter : struct, IPrimitiveWriter, allows ref struct
    {
        throw new NotImplementedException();
    }

    public static bool operator ==(Utf8String left, Utf8String right)
    {
        return left.Equals(right);
    }

    public static bool operator !=(Utf8String left, Utf8String right)
    {
        return !(left == right);
    }
}