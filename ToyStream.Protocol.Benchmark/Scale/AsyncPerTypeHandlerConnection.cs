namespace ToyStream.Protocol.Benchmark.Scale;

using System.Buffers;
using System.IO.Pipelines;
using System.Runtime.CompilerServices;
using System.Runtime.InteropServices;
using Messages;

/// <summary>
/// Shape A plus a real handler call: the per-type decode helper stays async, so every message type
/// still gets its own async state machine instantiation.
/// </summary>
public sealed class AsyncPerTypeHandlerConnection : IAsyncDisposable
{
    private readonly PipeReader reader;
    private readonly PipeWriter writer;
    private readonly CancellationToken cancellationToken;
    private readonly ScaleServerHandler handler;
    private readonly Task readerTask;

    public AsyncPerTypeHandlerConnection(PipeReader reader, PipeWriter writer, ScaleServerHandler handler,
        CancellationToken cancellationToken)
    {
        this.reader = reader;
        this.writer = writer;
        this.handler = handler;
        this.cancellationToken = cancellationToken;
        this.readerTask = this.Read();
    }

    private ValueTask Write<TMessage>(TMessage message)
        where TMessage : struct, IMessage<TMessage>, allows ref struct
    {
        PrimitiveWriter<PipeWriter> primitiveWriter = new(this.writer);
        primitiveWriter.Write(message.Type);
        message.Write(primitiveWriter);

        return Flush();

        [AsyncMethodBuilder(typeof(PoolingAsyncValueTaskMethodBuilder))]
        async ValueTask Flush() => await this.writer.FlushAsync(this.cancellationToken).ConfigureAwait(false);
    }

    private async Task Read()
    {
        await Task.Yield();
        var buffer = ReadOnlySequence<byte>.Empty;

        try
        {
            while (!this.cancellationToken.IsCancellationRequested)
            {
                if (buffer.Length < 1)
                {
                    var result = await this.reader.ReadAsync(this.cancellationToken).ConfigureAwait(false);
                    buffer = result.Buffer;
                }

                var typeByte = MemoryMarshal.Read<byte>(buffer.Slice(0, 1).FirstSpan);
                buffer = buffer.Slice(1);

// Awaited immediately, just outside the switch expression.
#pragma warning disable CA2012
                await (typeByte switch
                {
                    10 => Dispatch<ScaleMessage00>(this.handler),
                    11 => Dispatch<ScaleMessage01>(this.handler),
                    12 => Dispatch<ScaleMessage02>(this.handler),
                    13 => Dispatch<ScaleMessage03>(this.handler),
                    14 => Dispatch<ScaleMessage04>(this.handler),
                    15 => Dispatch<ScaleMessage05>(this.handler),
                    16 => Dispatch<ScaleMessage06>(this.handler),
                    17 => Dispatch<ScaleMessage07>(this.handler),
                    18 => Dispatch<ScaleMessage08>(this.handler),
                    19 => Dispatch<ScaleMessage09>(this.handler),
                    20 => Dispatch<ScaleMessage10>(this.handler),
                    21 => Dispatch<ScaleMessage11>(this.handler),
                    22 => Dispatch<ScaleMessage12>(this.handler),
                    23 => Dispatch<ScaleMessage13>(this.handler),
                    24 => Dispatch<ScaleMessage14>(this.handler),
                    25 => Dispatch<ScaleMessage15>(this.handler),
                    26 => Dispatch<ScaleMessage16>(this.handler),
                    27 => Dispatch<ScaleMessage17>(this.handler),
                    28 => Dispatch<ScaleMessage18>(this.handler),
                    29 => Dispatch<ScaleMessage19>(this.handler),
                    30 => Dispatch<ScaleMessage20>(this.handler),
                    31 => Dispatch<ScaleMessage21>(this.handler),
                    32 => Dispatch<ScaleMessage22>(this.handler),
                    33 => Dispatch<ScaleMessage23>(this.handler),
                    34 => Dispatch<ScaleMessage24>(this.handler),
                    35 => Dispatch<ScaleMessage25>(this.handler),
                    36 => Dispatch<ScaleMessage26>(this.handler),
                    37 => Dispatch<ScaleMessage27>(this.handler),
                    38 => Dispatch<ScaleMessage28>(this.handler),
                    39 => Dispatch<ScaleMessage29>(this.handler),
                    _ => throw new NotImplementedException()
                }).ConfigureAwait(false);
#pragma warning restore CA2012
            }
        }
        catch (OperationCanceledException)
        {
        }

        return;

        [AsyncMethodBuilder(typeof(PoolingAsyncValueTaskMethodBuilder))]
        async ValueTask Dispatch<TMessage>(IHandler<TMessage> messageHandler)
            where TMessage : struct, IMessage<TMessage>, allows ref struct
        {
            var primitiveReader = new PrimitiveReader(buffer);
            var read = TMessage.TryRead(ref primitiveReader, out var message, out var bytesToRead);

            while (!read)
            {
                this.reader.AdvanceTo(buffer.Start, buffer.End);
                var readResult = await this.reader.ReadAtLeastAsync(bytesToRead, this.cancellationToken).ConfigureAwait(false);
                buffer = readResult.Buffer;
                primitiveReader = new PrimitiveReader(buffer);
                read = TMessage.TryRead(ref primitiveReader, out message, out bytesToRead);
            }

            buffer = primitiveReader.Remaining;
            await messageHandler.Handle(in message).ConfigureAwait(false);

            this.reader.AdvanceTo(buffer.Start);
            await this.Write(new AcknowledgeMessage()).ConfigureAwait(false);
        }
    }

    public async ValueTask DisposeAsync()
    {
        await this.readerTask.ConfigureAwait(false);
        GC.SuppressFinalize(this);
    }
}
