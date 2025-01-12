namespace ToyStream.Shared;

using System.Buffers.Binary;
using System.Runtime.CompilerServices;

public ref struct SpanWriter
{
    private readonly Span<byte> span;
    
    public SpanWriter(Span<byte> span)
    {
        this.span = span;
    }

    public static implicit operator SpanWriter(Span<byte> span) => new(span);

    private int writtenBytes = 0;
    public Span<byte> Written => this.span[..this.writtenBytes];
    
    public Span<byte> Remaining => this.span.Slice(this.writtenBytes);

    public void Advance(int written)
    {
        this.writtenBytes += written;
    }

    public void Write(byte value)
    {
        this.Remaining[0] = value;
        Advance(1);
    }
    
    public void Write(short value)
    {
        BinaryPrimitives.WriteInt16BigEndian(this.Remaining, value);
        Advance(sizeof(short));
    }
    
    public void Write(int value)
    {
        BinaryPrimitives.WriteInt32BigEndian(this.Remaining, value);
        Advance(sizeof(int));
    }

    public void Write(long value)
    {
        BinaryPrimitives.WriteInt64BigEndian(this.Remaining, value);
        Advance(sizeof(long));
    }
    
    public void Write(Span<byte> value)
    {
        value.CopyTo(this.Remaining);
        Advance(value.Length);
    }
    
    public void Write<T>(T value) where T: struct, Enum
    {
        var size = Unsafe.SizeOf<T>();
        switch (size)
        {
            case 1: 
                this.Write(Unsafe.As<T, byte>(ref value));
                break;
            case 2: 
                this.Write(Unsafe.As<T, short>(ref value));
                break;
            case 4: 
                this.Write(Unsafe.As<T, int>(ref value));
                break;
            case 8:
                this.Write(Unsafe.As<T, long>(ref value));
                break;
        }
    }
}