using System.Buffers;

namespace MyStream.Protocol.Primitives;

public interface IPrimitiveWriter
{
    public void Write(byte value);
    public void Write(short value);
    public void Write(ushort value);
    public void Write(int value);
    public void Write(long value);
    public void Write(TimeSpan value);
    public void Write(ReadOnlySpan<byte> value);
    public void Write(ReadOnlySequence<byte> value);
    public void WriteEnum<TEnum>(TEnum value) where TEnum : unmanaged, Enum;
    public void Write<T>(T value) where T : struct, IProtocolPrimitive<T>, allows ref struct;
}