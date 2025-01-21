namespace ToyStream.Protocol;

using Shared;

internal interface IPrimitiveWriter
{
    public void Write(byte value);
    public void Write(short value);
    public void Write(int value);
    public void Write(long value);
    public void Write<TEnum>(TEnum value) where TEnum : struct, Enum;
    public void Write(RefUtf8String value);
}