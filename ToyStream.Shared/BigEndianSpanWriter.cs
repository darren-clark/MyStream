namespace ToyStream.Shared;

using System.Buffers;
using System.Buffers.Binary;
using System.Runtime.CompilerServices;

public ref struct BigEndianSpanWriter
{
    private readonly Span<byte> buffer;
    
    public BigEndianSpanWriter(Span<byte> buffer)
    {
        this.buffer = buffer;
    }

    public static implicit operator BigEndianSpanWriter(Span<byte> span) => new(span);

    private int writtenBytes = 0;
    
    public Span<byte> Written => this.buffer[..this.writtenBytes];
    
    public Span<byte> Remaining => this.buffer[this.writtenBytes..];

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
    
    public void Write(ReadOnlySpan<byte> value)
    {
        value.CopyTo(this.Remaining);
        Advance(value.Length);
    }
    
    public void Write(ReadOnlySequence<byte> value)
    {
        value.CopyTo(this.Remaining);
        Advance((int) value.Length);
    }
    
    public void Write(RefUtf8String value)
    {
        this.Write(value.Length);
        this.Write(value.Sequence);
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