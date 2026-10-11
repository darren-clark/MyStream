namespace ToyStream.Protocol;

using Shared;

public interface IPrimitiveWriter
{
    public void Write(byte value);
    public void Write(short value);
    public void Write(int value);
    public void Write(long value);
    public void Write<TEnum>(TEnum value) where TEnum : unmanaged, Enum;
    public void Write(RefUtf8String value);
}