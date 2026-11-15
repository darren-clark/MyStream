using System.Buffers;

namespace MyStream;

public interface IRefSerializable<T> where T: struct, allows ref struct
{
    public static abstract bool TryRead(SequenceReader<byte> reader, out T value, out long read, out long bytesNeeded);
}

