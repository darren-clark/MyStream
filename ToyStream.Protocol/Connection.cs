using System.Buffers;

namespace ToyStream.Protocol;

using System.IO.Pipelines;

/// <summary>
/// The whole read loop, shared by every role. Routing lives in THandler, so a client and a server
/// differ only by the handler they close over - and because THandler is a struct the JIT specialises
/// this type per role, which devirtualises the dispatch call.
/// </summary>
public sealed class Connection<THandler> where THandler : struct, IConnectionHandler
{
    private readonly PipeReader reader;
    private readonly PipeWriter writer;

    // Not readonly: a constraint cannot say "readonly struct", so an interface call on a readonly
    // field of THandler is defensively copied.
    private THandler handler;

    public Connection(PipeReader reader, PipeWriter writer, THandler handler)
    {
        this.reader = reader;
        this.writer = writer;
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
                while (true)
                {
                    var primitiveReader = new PrimitiveReader(buffer);
                    if (!primitiveReader.TryReadMessageType(out var messageType, out bytesNeeded))
                    {
                        break;
                    }

                    var dispatched = this.handler.TryParseAndDispatch(messageType,
                        ref primitiveReader, new PrimitiveWriter(this.writer), cancellationToken);

                    if (!dispatched.IsSuccess)
                    {
                        bytesNeeded = dispatched.BytesNeeded;
                        break;
                    }

                    buffer = primitiveReader.Remaining;

                    wroteResponse |= dispatched.WroteResponse;
                    if (!dispatched.Task.IsCompleted)
                    {
                        await dispatched.Task.ConfigureAwait(false);
                    }
                }

                // Advance only once every handler has consumed its message, which is a view over this buffer.
                this.reader.AdvanceTo(buffer.Start, buffer.End);

                // One flush per read rather than per message, so a batch of pipelined requests pays once.
                if (wroteResponse)
                {
                    var flush = this.writer.FlushAsync(cancellationToken);
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
}