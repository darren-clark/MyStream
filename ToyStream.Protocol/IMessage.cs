namespace ToyStream.Protocol;

using System.Buffers;
using Shared;

public interface IMessage<T> where T : struct, IMessage<T>, allows ref struct
{
    public int Size { get; }
    public MessageType Type { get; }
    
    public static abstract bool TryRead(scoped ref SequenceReader<byte> reader, out T message, out int bytesToRead);
    public void Write(ref BigEndianSpanWriter writer);
}