namespace ToyStream.Protocol.ReceiveRef;

using System.Buffers;
using System.Diagnostics.CodeAnalysis;


public interface IMessage<T> where T: struct, IMessage<T>, allows ref struct
{
    public static abstract bool TryRead(SequenceReader<byte> reader, out T component, out int size);    
}
