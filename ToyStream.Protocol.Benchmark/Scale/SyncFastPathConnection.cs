namespace ToyStream.Protocol.Benchmark.Scale;

using System.Buffers;
using System.IO.Pipelines;
using System.Runtime.CompilerServices;
using System.Runtime.InteropServices;
using Messages;

/// <summary>
/// Read loop shape B: the per-type decode helper is synchronous, so each message type costs a small
/// non-async instantiation instead of a full async state machine. Only the "need more bytes" path is
/// async, and it is shared by every message type rather than duplicated per type.
/// </summary>
public sealed class SyncFastPathConnection : IAsyncDisposable
{
    private readonly PipeReader reader;
    private readonly PipeWriter writer;
    private readonly CancellationToken cancellationToken;
    private readonly Task readerTask;

    public SyncFastPathConnection(PipeReader reader, PipeWriter writer, CancellationToken cancellationToken)
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

    // Synchronous, so a message type costs one small instantiation rather than a state machine.
    private static bool TryDecode<TMessage>(ref ReadOnlySequence<byte> body)
        where TMessage : struct, IMessage<TMessage>, allows ref struct
    {
        var primitiveReader = new PrimitiveReader(body);
        if (!TMessage.TryRead(ref primitiveReader, out _, out _))
        {
            return false;
        }

        body = primitiveReader.Remaining;
        return true;
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
                var body = buffer.Slice(1);

                var decoded = typeByte switch
                {
                    10 => TryDecode<ScaleMessage00>(ref body),
                    11 => TryDecode<ScaleMessage01>(ref body),
                    12 => TryDecode<ScaleMessage02>(ref body),
                    13 => TryDecode<ScaleMessage03>(ref body),
                    14 => TryDecode<ScaleMessage04>(ref body),
                    15 => TryDecode<ScaleMessage05>(ref body),
                    16 => TryDecode<ScaleMessage06>(ref body),
                    17 => TryDecode<ScaleMessage07>(ref body),
                    18 => TryDecode<ScaleMessage08>(ref body),
                    19 => TryDecode<ScaleMessage09>(ref body),
                    20 => TryDecode<ScaleMessage10>(ref body),
                    21 => TryDecode<ScaleMessage11>(ref body),
                    22 => TryDecode<ScaleMessage12>(ref body),
                    23 => TryDecode<ScaleMessage13>(ref body),
                    24 => TryDecode<ScaleMessage14>(ref body),
                    25 => TryDecode<ScaleMessage15>(ref body),
                    26 => TryDecode<ScaleMessage16>(ref body),
                    27 => TryDecode<ScaleMessage17>(ref body),
                    28 => TryDecode<ScaleMessage18>(ref body),
                    29 => TryDecode<ScaleMessage19>(ref body),
                    30 => TryDecode<ScaleMessage20>(ref body),
                    31 => TryDecode<ScaleMessage21>(ref body),
                    32 => TryDecode<ScaleMessage22>(ref body),
                    33 => TryDecode<ScaleMessage23>(ref body),
                    34 => TryDecode<ScaleMessage24>(ref body),
                    35 => TryDecode<ScaleMessage25>(ref body),
                    36 => TryDecode<ScaleMessage26>(ref body),
                    37 => TryDecode<ScaleMessage27>(ref body),
                    38 => TryDecode<ScaleMessage28>(ref body),
                    39 => TryDecode<ScaleMessage29>(ref body),
                    _ => throw new NotImplementedException()
                };

                if (!decoded)
                {
                    // One async slow path shared by every message type, rather than one per type.
                    this.reader.AdvanceTo(buffer.Start, buffer.End);
                    var readResult = await this.reader.ReadAsync(this.cancellationToken).ConfigureAwait(false);
                    buffer = readResult.Buffer;
                    continue;
                }

                buffer = body;
                this.reader.AdvanceTo(buffer.Start);
                await this.Write(new AcknowledgeMessage()).ConfigureAwait(false);
            }
        }
        catch (OperationCanceledException)
        {
        }
    }

    public async ValueTask DisposeAsync()
    {
        await this.readerTask.ConfigureAwait(false);
        GC.SuppressFinalize(this);
    }
}
