namespace ToyStream.Protocol;

using System.Buffers;
using System.Buffers.Binary;
using System.Runtime.CompilerServices;
using ToyStream.Shared;

internal ref struct PrimitiveWriter<T>: IPrimitiveWriter where T: IBufferWriter<byte>
{
    private readonly T writer;
    
    [MethodImpl(MethodImplOptions.AggressiveInlining)]
    public PrimitiveWriter(T writer)
    {
        this.writer = writer;
    }

    [MethodImpl(MethodImplOptions.AggressiveInlining)]
    public static implicit operator PrimitiveWriter<T>(T writer) => new(writer);

    [MethodImpl(MethodImplOptions.AggressiveInlining)]
    public void Write(byte value)
    {
        this.writer.Write(new ReadOnlySpan<byte>(ref value));
    }
    
    [MethodImpl(MethodImplOptions.AggressiveInlining)]
    public void Write(short value)
    {
        Span<byte> span = stackalloc byte[sizeof(short)];
        BinaryPrimitives.WriteInt16BigEndian(span, value);
        this.writer.Write(span);
    }
    
    [MethodImpl(MethodImplOptions.AggressiveInlining)]
    public void Write(int value)
    {
        Span<byte> buffer = stackalloc byte[sizeof(int)];
        BinaryPrimitives.WriteInt32BigEndian(buffer, value);
        this.writer.Write(buffer);
    }

    [MethodImpl(MethodImplOptions.AggressiveInlining)]
    public void Write(long value)
    {
        Span<byte> buffer = stackalloc byte[sizeof(long)];
        BinaryPrimitives.WriteInt64BigEndian(buffer, value);
        this.writer.Write(buffer);
    }
    
    [MethodImpl(MethodImplOptions.AggressiveInlining)]
    private void Write(ReadOnlySpan<byte> value)
    {
        this.writer.Write(value);
    }
    
    [MethodImpl(MethodImplOptions.AggressiveInlining)]
    private void Write(ReadOnlySequence<byte> value)
    {
        while (!value.IsSingleSegment)
        {
            this.writer.Write(value.FirstSpan);
            value = value.Slice(value.FirstSpan.Length);
        }
        this.writer.Write(value.FirstSpan);
    }
    
    [MethodImpl(MethodImplOptions.AggressiveInlining)]
    public void Write(RefUtf8String value)
    {
        this.Write(value.Length);
        this.Write(value.Sequence);
    }
    
    [MethodImpl(MethodImplOptions.AggressiveInlining)]
    public void Write<TEnum>(TEnum value) where TEnum: struct, Enum
    {
        var size = Unsafe.SizeOf<TEnum>();
        switch (size)
        {
            case 1: 
                this.Write(Unsafe.As<TEnum, byte>(ref value));
                break;
            case 2: 
                this.Write(Unsafe.As<TEnum, short>(ref value));
                break;
            case 4: 
                this.Write(Unsafe.As<TEnum, int>(ref value));
                break;
            case 8:
                this.Write(Unsafe.As<TEnum, long>(ref value));
                break;
        }
    }
}