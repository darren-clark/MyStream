namespace ToyStream.Protocol.ReceiveRef;

using System.Buffers;
using System.Diagnostics.CodeAnalysis;

public interface IProtocolComponent<T> where T: struct, IProtocolComponent<T>, allows ref struct
{
    /// <summary>
    /// If there is enough memory to read:
    ///   Sets <paramref name="component"/> to a new message component, <paramref name="bytesToRead"/>
    ///   The reader will be advanced past the bytes read.
    /// else
    ///   Returns a <see cref="ComponentCreateResult{T}"/> result with the additional number of bytes required
    ///   The buffer reference should be unchanged.
    /// </summary>
    /// <returns></returns>
    /// <remarks>
    /// Note that even if the provided number of bytes are read, there is no guarantee a subsequent
    /// call will succeed. It's possible that the full length could not be determined
    /// without additional data. 
    /// </remarks>
    public static abstract bool TryRead(scoped ref SequenceReader<byte> reader, out T component, out int bytesToRead);
}