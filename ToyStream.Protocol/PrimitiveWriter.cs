namespace ToyStream.Protocol;

using System.Buffers;
using System.Buffers.Binary;
using System.Runtime.CompilerServices;
using ToyStream.Shared;

internal static class PrimitiveWriter
{
    [MethodImpl(MethodImplOptions.AggressiveInlining)]
    public static PrimitiveWriter<T> Create<T>(T writer) where T: IBufferWriter<byte> => new(writer);
}

[method: MethodImpl(MethodImplOptions.AggressiveInlining)]
internal readonly struct PrimitiveWriter<T>(T writer) : IPrimitiveWriter
    where T : IBufferWriter<byte>
{
    [MethodImpl(MethodImplOptions.AggressiveInlining)]
    public static implicit operator PrimitiveWriter<T>(T writer) => new(writer);

    [MethodImpl(MethodImplOptions.AggressiveInlining)]
    public void Write(byte value)
    {
        writer.Write(new ReadOnlySpan<byte>(ref value));
    }
    
    [MethodImpl(MethodImplOptions.AggressiveInlining)]
    public void Write(short value)
    {
        Span<byte> span = stackalloc byte[sizeof(short)];
        BinaryPrimitives.WriteInt16BigEndian(span, value);
        writer.Write(span);
    }
    
    [MethodImpl(MethodImplOptions.AggressiveInlining)]
    public void Write(int value)
    {
        Span<byte> buffer = stackalloc byte[sizeof(int)];
        BinaryPrimitives.WriteInt32BigEndian(buffer, value);
        writer.Write(buffer);
    }

    [MethodImpl(MethodImplOptions.AggressiveInlining)]
    public void Write(long value)
    {
        Span<byte> buffer = stackalloc byte[sizeof(long)];
        BinaryPrimitives.WriteInt64BigEndian(buffer, value);
        writer.Write(buffer);
    }
    
    [MethodImpl(MethodImplOptions.AggressiveInlining)]
    private void Write(ReadOnlySpan<byte> value)
    {
        writer.Write(value);
    }
    
    [MethodImpl(MethodImplOptions.AggressiveInlining)]
    private void Write(ReadOnlySequence<byte> value)
    {
        while (!value.IsSingleSegment)
        {
            writer.Write(value.FirstSpan);
            value = value.Slice(value.FirstSpan.Length);
        }
        writer.Write(value.FirstSpan);
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