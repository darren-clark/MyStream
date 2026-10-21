namespace ToyStream.Protocol;

using System.Buffers;
using System.Diagnostics;
using System.Diagnostics.CodeAnalysis;
using System.IO.Pipelines;

/// <summary>
/// Presents one message's chunked body as a plain <see cref="PipeReader"/>, so a handler reads a
/// payload the same way it would read any pipe and never sees the framing.
/// </summary>
/// <remarks>
/// One per connection, reset per message, so handing a handler its body allocates nothing. Buffers
/// are the connection's pipe memory and stay valid only until the next read, exactly as for a pipe.
/// </remarks>
public sealed class MessageBodyReader(PipeReader inner) : PipeReader
{
    private ReadOnlySequence<byte> buffer;
    private long? declaredSize;
    private long chunkRemaining;
    private long delivered;
    private bool finished;

    /// <summary>Takes over the connection's buffer at the first chunk header.</summary>
    internal void Begin(long? declaredSize, ReadOnlySequence<byte> remaining)
    {
        this.buffer = remaining;
        this.declaredSize = declaredSize;
        this.chunkRemaining = 0;
        this.delivered = 0;
        this.finished = false;
    }

    public long? DeclaredSize => this.declaredSize;
    
    /// <summary>
    /// Consumes whatever the handler left, so a handler that stops early costs the connection its
    /// position rather than desynchronising the stream, then hands the buffer back.
    /// </summary>
    internal async ValueTask<ReadOnlySequence<byte>> FinishAsync(CancellationToken cancellationToken)
    {
        while (!this.finished)
        {
            var result = await this.ReadAsync(cancellationToken).ConfigureAwait(false);
            if (result.Buffer.IsEmpty)
            {
                break;
            }

            this.AdvanceTo(result.Buffer.End);
        }

        if (declaredSize.HasValue && this.delivered != declaredSize)
        {
            ThrowBodySizeMismatch(declaredSize.Value, this.delivered);
        }

        return this.buffer;
    }

    public override ValueTask<ReadResult> ReadAsync(CancellationToken cancellationToken = default)
    {
        if (this.chunkRemaining > 0 && !this.buffer.IsEmpty)
        {
            return new ValueTask<ReadResult>(this.CurrentChunk());
        }

        return this.finished
            ? new ValueTask<ReadResult>(EndOfBody)
            : this.ReadNextChunkAsync(cancellationToken);
    }

    public override bool TryRead(out ReadResult result)
    {
        if (this.chunkRemaining > 0 && !this.buffer.IsEmpty)
        {
            result = this.CurrentChunk();
            return true;
        }

        result = EndOfBody;
        return this.finished;
    }

    public override void AdvanceTo(SequencePosition consumed) => this.AdvanceTo(consumed, consumed);

    /// <summary>
    /// The examined position is deliberately ignored: the connection releases pipe memory when it
    /// refills, so advancing the inner reader here would invalidate the buffer still being handed out.
    /// </summary>
    public override void AdvanceTo(SequencePosition consumed, SequencePosition examined)
    {
        // The canonical pipe loop advances past the final result too, but that one is empty and its
        // positions are not ours: slicing by them would corrupt what the connection reads next.
        if (this.finished)
        {
            return;
        }

        var consumedLength = this.buffer.Slice(this.buffer.Start, consumed).Length;
        this.chunkRemaining -= consumedLength;
        this.delivered += consumedLength;
        this.buffer = this.buffer.Slice(consumed);
    }

    /// <summary>The connection owns the underlying pipe, so a handler completing its body is a no-op.</summary>
    public override void Complete(Exception? exception = null)
    {
    }

    public override void CancelPendingRead() => inner.CancelPendingRead();

    private ReadResult CurrentChunk()
    {
        var chunk = this.buffer.Length <= this.chunkRemaining
            ? this.buffer
            : this.buffer.Slice(0, this.chunkRemaining);

        return new ReadResult(chunk, isCanceled: false, isCompleted: false);
    }

    private static ReadResult EndOfBody =>
        new(ReadOnlySequence<byte>.Empty, isCanceled: false, isCompleted: true);

    private async ValueTask<ReadResult> ReadNextChunkAsync(CancellationToken cancellationToken)
    {
        while (true)
        {
            if (this.chunkRemaining == 0)
            {
                if (this.buffer.Length < sizeof(int))
                {
                    await this.RefillAsync(sizeof(int), cancellationToken).ConfigureAwait(false);
                    continue;
                }

                var headerReader = new PrimitiveReader(this.buffer);
                headerReader.TryReadInt32(out var chunkLength, out _);
                this.buffer = headerReader.Remaining;

                if (chunkLength == 0)
                {
                    this.finished = true;
                    return EndOfBody;
                }

                // Unsigned so a negative length is rejected by the same comparison as an oversized one.
                if ((uint)chunkLength > ProtocolConstants.MaxChunkSize)
                {
                    ThrowInvalidChunkLength(chunkLength);
                }

                this.chunkRemaining = chunkLength;
                continue;
            }

            if (this.buffer.IsEmpty)
            {
                await this.RefillAsync(1, cancellationToken).ConfigureAwait(false);
                continue;
            }

            return this.CurrentChunk();
        }
    }

    /// <summary>Releases what the handler has already consumed, then waits for more of the body.</summary>
    private async ValueTask RefillAsync(int minimum, CancellationToken cancellationToken)
    {
        inner.AdvanceTo(this.buffer.Start, this.buffer.End);

        var result = await inner.ReadAtLeastAsync(minimum, cancellationToken).ConfigureAwait(false);
        if (result.IsCompleted && result.Buffer.Length < minimum)
        {
            ThrowTruncatedPayload(minimum - result.Buffer.Length);
        }

        this.buffer = result.Buffer;
    }

    [DoesNotReturn]
    [StackTraceHidden]
    private static void ThrowTruncatedPayload(long missing) =>
        throw new InvalidOperationException($"Connection closed with {missing} payload bytes outstanding.");

    [DoesNotReturn]
    [StackTraceHidden]
    private static void ThrowInvalidChunkLength(int length) =>
        throw new InvalidOperationException(
            $"Chunk length {length} is outside 0..{ProtocolConstants.MaxChunkSize}.");

    [DoesNotReturn]
    [StackTraceHidden]
    private static void ThrowBodySizeMismatch(long declared, long delivered) =>
        throw new InvalidOperationException(
            $"Body declared {declared} bytes but {delivered} were framed.");
}
