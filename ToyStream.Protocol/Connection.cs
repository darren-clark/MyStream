using System.Buffers;

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
}