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
    private readonly MessageBodyReader bodyReader;

    // Not readonly: a constraint cannot say "readonly struct", so an interface call on a readonly
    // field of THandler is defensively copied.
    private THandler handler;

    public Connection(PipeReader reader, PipeWriter writer, THandler handler)
        : base(writer)
    {
        this.reader = reader;
        this.handler = handler;
        this.bodyReader = new MessageBodyReader(reader);
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

                    if (ProtocolConstants.IsResponse(typeByte))
                    {
                        if (this.TryParseAndComplete((ResponseType)typeByte, ref primitiveReader, out bytesNeeded))
                        {
                            buffer = primitiveReader.Remaining;
                            continue;
                        }

                        break;
                    }
                    
                    // Incoming request path.
                    var dispatch = new Dispatcher(primitiveReader, new PrimitiveWriter(this.Writer),
                        this.bodyReader, cancellationToken);

                    var dispatched = this.handler.TryParseAndDispatch((MessageType)typeByte, ref dispatch);

                    if (!dispatched.IsSuccess)
                    {
                        bytesNeeded = dispatched.BytesNeeded;
                        break;
                    }

                    if (dispatched.OwnsBody)
                    {
                        // The handler may already have refilled the pipe and recycled the segments
                        // the dispatch reader points into, so the body reader's buffer is the only
                        // valid one from here.
                        await dispatched.Task.ConfigureAwait(false);
                        buffer = await this.bodyReader
                            .FinishAsync(dispatched.DeclaredBodySize, cancellationToken)
                            .ConfigureAwait(false);

                        wroteResponse = true;
                        streamed = true;
                        break;
                    }

                    buffer = dispatch.Remaining;

                    wroteResponse |= dispatched.WroteResponse;
                    if (!dispatched.Task.IsCompleted)
                    {
                        await dispatched.Task.ConfigureAwait(false);
                    }
                }

                // Advance only once every handler has consumed its message, which is a view over this buffer.
                this.reader.AdvanceTo(buffer.Start, buffer.End);
                if (streamed)
                {
                    buffer = ReadOnlySequence<byte>.Empty;
                    bytesNeeded = 1;
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
}