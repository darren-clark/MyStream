using System.Buffers;
using System.Diagnostics;
using System.Diagnostics.CodeAnalysis;

namespace ToyStream.Protocol;

using System.IO.Pipelines;

/// <summary>
/// The read loop, shared by every role. Request routing lives in THandler, and because THandler is a
/// struct the JIT specialises this type per role, which devirtualises the dispatch call. Responses
/// never reach the handler - the base matches them to the request that is outstanding.
/// </summary>
public sealed class Connection<THandler> : Connection where THandler : struct, IConnectionHandler
{
    private readonly PipeReader reader;

    // Not readonly: a constraint cannot say "readonly struct", so an interface call on a readonly
    // field of THandler is defensively copied.
    private THandler handler;

    public Connection(PipeReader reader, PipeWriter writer, THandler handler)
        : base(writer)
    {
        this.reader = reader;
        this.handler = handler;
    }

    public async Task Start(CancellationToken cancellationToken)
    {
        await Task.Yield();
        try
        {
            var buffer = ReadOnlySequence<byte>.Empty;
            var bytesNeeded = 1;
            while (!cancellationToken.IsCancellationRequested)
            {
                int bytesToRead = (int)buffer.Length + bytesNeeded;
                // ReadAtLeastAsync already completes synchronously when the data is buffered, so a
                // TryRead probe in front of it only adds a call and a second path through the loop.
                var result = await this.reader.ReadAtLeastAsync(bytesToRead, cancellationToken)
                    .ConfigureAwait(false);

                buffer = result.Buffer;
                // Our input is gone.
                if (result.IsCompleted && buffer.Length < bytesToRead)
                {
                    break;
                }

                var wroteResponse = false;
                var streamed = false;
                while (true)
                {
                    var primitiveReader = new PrimitiveReader(buffer);
                    if (!primitiveReader.TryReadByte(out var typeByte, out bytesNeeded))
                    {
                        break;
                    }

                    var dispatched = ProtocolConstants.IsResponse(typeByte)
                        ? this.TryParseAndComplete((ResponseType)typeByte, ref primitiveReader)
                        : this.handler.TryParseAndDispatch((MessageType)typeByte, ref primitiveReader,
                            new PrimitiveWriter(this.Writer), cancellationToken);

                    if (!dispatched.IsSuccess)
                    {
                        bytesNeeded = dispatched.BytesNeeded;
                        break;
                    }

                    buffer = primitiveReader.Remaining;

                    if (dispatched.Receiver is not null)
                    {
                        // The pump advances the reader itself, so this batch ends here.
                        await this.PumpPayload(buffer, dispatched.Receiver, dispatched.DeclaredBodySize,
                                cancellationToken)
                            .ConfigureAwait(false);

                        buffer = ReadOnlySequence<byte>.Empty;
                        bytesNeeded = 1;
                        wroteResponse = true;
                        streamed = true;
                        break;
                    }

                    wroteResponse |= dispatched.WroteResponse;
                    if (!dispatched.Task.IsCompleted)
                    {
                        await dispatched.Task.ConfigureAwait(false);
                    }
                }

                // Advance only once every handler has consumed its message, which is a view over this buffer.
                if (!streamed)
                {
                    this.reader.AdvanceTo(buffer.Start, buffer.End);
                }

                // One flush per read rather than per message, so a batch of pipelined requests pays once.
                if (wroteResponse)
                {
                    var flush = this.Writer.FlushAsync(cancellationToken);
                    if (flush.IsCompletedSuccessfully)
                    {
                        flush.GetAwaiter().GetResult();
                    }
                    else
                    {
                        await flush.ConfigureAwait(false);
                    }
                }
            }
        }
        catch (OperationCanceledException)
        {
        }
    }

    /// <summary>
    /// Feeds a chunked body to its receiver straight from pipe memory, ending at the zero-length
    /// chunk. Consumed bytes are released back to the pipe on each refill, so a receiver that keeps
    /// bytes must copy them.
    /// </summary>
    /// <remarks>
    /// The chunk framing alone decides where the body ends. A declared size is only cross-checked
    /// against it afterwards, so a header that disagrees cannot desynchronise the stream.
    /// </remarks>
    private async ValueTask PumpPayload(ReadOnlySequence<byte> buffer, IPayloadReceiver receiver,
        long declaredSize, CancellationToken cancellationToken)
    {
        var chunkRemaining = 0L;
        var delivered = 0L;

        while (true)
        {
            if (chunkRemaining == 0)
            {
                if (buffer.Length < sizeof(int))
                {
                    buffer = await this.RefillPayload(buffer, sizeof(int), cancellationToken)
                        .ConfigureAwait(false);
                    continue;
                }

                var headerReader = new PrimitiveReader(buffer);
                headerReader.TryReadInt32(out var chunkLength, out _);
                buffer = headerReader.Remaining;

                if (chunkLength == 0)
                {
                    break;
                }

                // Unsigned so a negative length is rejected by the same comparison as an oversized one.
                if ((uint)chunkLength > ProtocolConstants.MaxChunkSize)
                {
                    ThrowInvalidChunkLength(chunkLength);
                }

                chunkRemaining = chunkLength;
                continue;
            }

            if (buffer.IsEmpty)
            {
                buffer = await this.RefillPayload(buffer, 1, cancellationToken).ConfigureAwait(false);
                continue;
            }

            var chunk = buffer.Length <= chunkRemaining ? buffer : buffer.Slice(0, chunkRemaining);
            await receiver.ReceiveAsync(chunk, cancellationToken).ConfigureAwait(false);
            chunkRemaining -= chunk.Length;
            delivered += chunk.Length;
            buffer = buffer.Slice(chunk.End);
        }

        if (declaredSize >= 0 && delivered != declaredSize)
        {
            ThrowBodySizeMismatch(declaredSize, delivered);
        }

        this.reader.AdvanceTo(buffer.Start, buffer.End);
        await receiver.CompleteAsync(new PrimitiveWriter(this.Writer), cancellationToken)
            .ConfigureAwait(false);
    }

    /// <summary>Releases what the receiver has already seen, then waits for more of the body.</summary>
    private async ValueTask<ReadOnlySequence<byte>> RefillPayload(ReadOnlySequence<byte> buffer, int minimum,
        CancellationToken cancellationToken)
    {
        this.reader.AdvanceTo(buffer.Start, buffer.End);

        var result = await this.reader.ReadAtLeastAsync(minimum, cancellationToken).ConfigureAwait(false);
        if (result.IsCompleted && result.Buffer.Length < minimum)
        {
            ThrowTruncatedPayload(minimum - result.Buffer.Length);
        }

        return result.Buffer;
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