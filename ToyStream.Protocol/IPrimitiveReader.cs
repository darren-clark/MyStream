namespace ToyStream.Protocol;

using Shared;

internal interface IPrimitiveReader
{
    public bool TryReadByte(out byte value, out int bytesToRead);
    public bool TryReadInt16(out short value, out int bytesToRead);
    public bool TryReadInt32(out int value, out int bytesToRead);
    public bool TryReadInt64(out long value, out int bytesToRead);
    public bool TryReadUtf8String(out RefUtf8String value, out int bytesToRead);
    public bool TryReadBytes(int maximum, out RefBytes value, out int bytesToRead);
    public bool TryReadEnum<T>(out T value, out int bytesToRead) where T : unmanaged, Enum;
}