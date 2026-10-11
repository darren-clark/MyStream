namespace ToyStream.Protocol;

using System.Buffers;
using Shared;

internal interface IPrimitiveReader
{
    public bool TryReadInt16(out short value, out int bytesToRead);
    public bool TryReadUtf8String(out RefUtf8String value, out int bytesToRead);
    public bool TryReadMessageType(out MessageType messageType, out int bytesToRead);
    public bool TryReadEnum<T>(out T value, out int bytesToRead) where T : unmanaged, Enum;
    public ReadOnlySequence<byte> Remaining { get; }
}