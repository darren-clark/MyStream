using System.Buffers;
using System.Diagnostics;
using System.IO.Pipelines;

namespace ToyStream.Protocol;

/// <summary>
/// Writes a chunked body, so a producer can emit one without knowing its length. The mirror of
/// <see cref="MessageBodyReader"/>: bytes written here come back out of that as a plain pipe.
/// </summary>
/// <remarks>
/// A PipeWriter so that anything able to write to a pipe - <see cref="PrimitiveWriter"/> included -
/// works against it unchanged and never sees the framing.
/// </remarks>
public sealed class MessageBodyWriter : PipeWriter
{
    private readonly PipeWriter inner;
    private byte[] buffer;
    private int pending;
    private bool completed;

    public MessageBodyWriter(PipeWriter inner, int chunkSize = 64 * 1024)
    {
        ArgumentOutOfRangeException.ThrowIfLessThan(chunkSize, sizeof(int));
        ArgumentOutOfRangeException.ThrowIfGreaterThan(chunkSize, ProtocolConstants.MaxChunkSize);

        this.inner = inner;
        this.buffer = ArrayPool<byte>.Shared.Rent(chunkSize);
    }

    public override Span<byte> GetSpan(int sizeHint = 0)
    {
        EnsureRoom(sizeHint);
        return this.buffer.AsSpan(this.pending);
    }

    public override Memory<byte> GetMemory(int sizeHint = 0)
    {
        EnsureRoom(sizeHint);
        return this.buffer.AsMemory(this.pending);
    }

    public override void Advance(int bytes) => this.pending += bytes;

    public override ValueTask<FlushResult> FlushAsync(CancellationToken cancellationToken = default)
    {
        EmitChunk();
        return this.inner.FlushAsync(cancellationToken);
    }

    /// <summary>Writes the zero length chunk that ends the body, then flushes what is left.</summary>
    public ValueTask<FlushResult> CompleteBodyAsync(CancellationToken cancellationToken = default)
    {
        EmitChunk();
        WriteLength(0);
        this.completed = true;
        return this.inner.FlushAsync(cancellationToken);
    }

    public override void CancelPendingFlush() => this.inner.CancelPendingFlush();

    /// <summary>The connection owns the pipe underneath, so completing here only returns the buffer.</summary>
    public override void Complete(Exception? exception = null)
    {
        Debug.Assert(this.completed || exception is not null, "a body must be completed before it is released");

        if (this.buffer.Length > 0)
        {
            ArrayPool<byte>.Shared.Return(this.buffer);
            this.buffer = [];
        }
    }

    private void EnsureRoom(int sizeHint)
    {
        // A hint larger than the chunk is honoured by growing, so a caller that needs one span gets it.
        if (sizeHint > this.buffer.Length)
        {
            EmitChunk();
            ArrayPool<byte>.Shared.Return(this.buffer);
            this.buffer = ArrayPool<byte>.Shared.Rent(Math.Min(sizeHint, ProtocolConstants.MaxChunkSize));
            return;
        }

        if (this.pending == this.buffer.Length || this.buffer.Length - this.pending < Math.Max(sizeHint, 1))
        {
            EmitChunk();
        }
    }

    private void EmitChunk()
    {
        if (this.pending == 0)
        {
            return;
        }

        WriteLength(this.pending);
        this.inner.Write(this.buffer.AsSpan(0, this.pending));
        this.pending = 0;
    }

    private void WriteLength(int length) => new PrimitiveWriter(this.inner).Write(length);
}
