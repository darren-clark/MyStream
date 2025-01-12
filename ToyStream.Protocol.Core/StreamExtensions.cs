namespace ToyStream.Protocol;

using System.Buffers.Binary;

public static class StreamExtensions
{
    public static void WriteInt16BigEndianAsync(this Stream stream, short value)
    {
        Span<byte> buffer = stackalloc byte[sizeof(short)];
        BinaryPrimitives.WriteInt16BigEndian(buffer, value);
        stream.Write(buffer);
    }
}