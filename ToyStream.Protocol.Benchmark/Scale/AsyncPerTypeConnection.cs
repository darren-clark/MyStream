namespace ToyStream.Protocol.Benchmark.Scale;

using System.Buffers;
using System.IO.Pipelines;
using System.Runtime.CompilerServices;
using System.Runtime.InteropServices;
using Messages;

/// <summary>
/// Read loop shape A, as the protocol does it today: the per-type decode helper is async, so every
/// message type gets its own async state machine instantiation.
/// </summary>
public sealed class AsyncPerTypeConnection : IAsyncDisposable
{
    private readonly PipeReader reader;
    private readonly PipeWriter writer;
    private readonly CancellationToken cancellationToken;
    private readonly Task readerTask;

    public AsyncPerTypeConnection(PipeReader reader, PipeWriter writer, CancellationToken cancellationToken)
    {
        this.reader = reader;
        this.writer = writer;
        this.cancellationToken = cancellationToken;
        this.readerTask = this.Read();
    }

    private ValueTask Write<TMessage>(TMessage message)
        where TMessage : struct, IMessage<TMessage>, allows ref struct
    {
        PrimitiveWriter<PipeWriter> primitiveWriter = new(this.writer);
        primitiveWriter.Write(TMessage.Type);
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
                    10 => Dispatch<ScaleMessage00>(),
                    11 => Dispatch<ScaleMessage01>(),
                    12 => Dispatch<ScaleMessage02>(),
                    13 => Dispatch<ScaleMessage03>(),
                    14 => Dispatch<ScaleMessage04>(),
                    15 => Dispatch<ScaleMessage05>(),
                    16 => Dispatch<ScaleMessage06>(),
                    17 => Dispatch<ScaleMessage07>(),
                    18 => Dispatch<ScaleMessage08>(),
                    19 => Dispatch<ScaleMessage09>(),
                    20 => Dispatch<ScaleMessage10>(),
                    21 => Dispatch<ScaleMessage11>(),
                    22 => Dispatch<ScaleMessage12>(),
                    23 => Dispatch<ScaleMessage13>(),
                    24 => Dispatch<ScaleMessage14>(),
                    25 => Dispatch<ScaleMessage15>(),
                    26 => Dispatch<ScaleMessage16>(),
                    27 => Dispatch<ScaleMessage17>(),
                    28 => Dispatch<ScaleMessage18>(),
                    29 => Dispatch<ScaleMessage19>(),
                    30 => Dispatch<ScaleMessage20>(),
                    31 => Dispatch<ScaleMessage21>(),
                    32 => Dispatch<ScaleMessage22>(),
                    33 => Dispatch<ScaleMessage23>(),
                    34 => Dispatch<ScaleMessage24>(),
                    35 => Dispatch<ScaleMessage25>(),
                    36 => Dispatch<ScaleMessage26>(),
                    37 => Dispatch<ScaleMessage27>(),
                    38 => Dispatch<ScaleMessage28>(),
                    39 => Dispatch<ScaleMessage29>(),
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
        async ValueTask Dispatch<TMessage>()
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
