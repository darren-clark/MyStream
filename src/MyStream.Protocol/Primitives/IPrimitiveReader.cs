using System.Buffers;

namespace MyStream.Protocol.Primitives;

public interface IPrimitiveReader
{
    /// <summary>What is left unread, so a view can be taken over a region that was just scanned.</summary>
    public ReadOnlySequence<byte> Remaining { get; }

    long Consumed { get; }
    public ReadOnlySequence<byte> GetRead();
    public void Advance(long position);

    public bool TryReadByte(out byte value, out int bytesToRead);
    public bool TryReadInt16(out short value, out int bytesToRead);
    public bool TryReadUInt16(out ushort value, out int bytesToRead);
    public bool TryReadInt32(out int value, out int bytesToRead);
    public bool TryReadInt64(out long value, out int bytesToRead);
//    public bool TryReadUtf8String(out RefUtf8String value, out int bytesToRead);
//    public bool TryReadBytes(int maximum, out RefBytes value, out int bytesToRead);

    /// <summary>As <see cref="TryReadBytes"/>, but left as a sequence for a non ref struct to hold.</summary>
    public bool TryReadLengthPrefixed(int maximum, out System.Buffers.ReadOnlySequence<byte> value,
        out int bytesToRead);

    /// <summary>A run of bytes whose length the caller already knows, left as a view over the buffer.</summary>
    public bool TryReadExact(int length, out System.Buffers.ReadOnlySequence<byte> value, out int bytesToRead);
    public bool TryReadEnum<T>(out T value, out int bytesToRead) where T : unmanaged, Enum;
    public bool TryReadPrimitive<T>(out T value, out int bytesToRead) where T: struct, IProtocolPrimitive<T>, allows ref struct;
}