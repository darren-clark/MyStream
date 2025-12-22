namespace ToyStream.Protocol;

using System.Buffers;
using Shared;

internal interface IPrimitiveReader
{
    public bool TryReadEnum<TEnum>(out TEnum value, out int bytesToRead) where TEnum : struct,Enum;
    public bool TryReadInt16(out short value, out int bytesToRead);
    public bool TryReadInt64(out long value, out int bytesToRead);
    public bool TryReadUtf8String(out RefUtf8String value, out int bytesToRead);
    public ReadOnlySequence<byte> Remaining { get; }
}