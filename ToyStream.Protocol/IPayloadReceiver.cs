namespace ToyStream.Protocol;

using System.Buffers;

/// <summary>
/// Receives a message payload in chunks. The connection keeps ownership of the pipe and feeds this,
/// so a handler is never given the reader and cannot desynchronise the stream.
/// </summary>
/// <remarks>
/// Chunks are views over pipe memory that the connection advances past as soon as
/// <see cref="ReceiveAsync"/> returns. A receiver that needs to keep bytes must copy them.
/// </remarks>
public interface IPayloadReceiver
{
    ValueTask ReceiveAsync(ReadOnlySequence<byte> chunk, CancellationToken cancellationToken);

    /// <summary>Called once the whole payload has been delivered; writes the response.</summary>
    ValueTask CompleteAsync(PrimitiveWriter writer, CancellationToken cancellationToken);
}
