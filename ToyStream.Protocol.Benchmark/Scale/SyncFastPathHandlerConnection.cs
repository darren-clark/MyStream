namespace ToyStream.Protocol.Benchmark.Scale;

using System.Buffers;
using System.IO.Pipelines;
using System.Runtime.CompilerServices;
using System.Runtime.InteropServices;
using Messages;

/// <summary>
/// Shape B plus a real handler call. The per-type helper calls the handler but does not await it,
/// so it stays synchronous: only the shared loop body awaits, and only the "need more bytes" path
/// is async. Calling the handler inside the synchronous portion also guarantees the message is a
/// valid view over the pipe buffer for exactly as long as the handler can see it.
/// </summary>
public sealed class SyncFastPathHandlerConnection : IAsyncDisposable
{
    private readonly PipeReader reader;
    private readonly PipeWriter writer;
    private readonly CancellationToken cancellationToken;
    private readonly ScaleServerHandler handler;
    private readonly Task readerTask;

    public SyncFastPathHandlerConnection(PipeReader reader, PipeWriter writer, ScaleServerHandler handler,
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
        primitiveWriter.Write(TMessage.Type);
        message.Write(primitiveWriter);

        return Flush();

        [AsyncMethodBuilder(typeof(PoolingAsyncValueTaskMethodBuilder))]
        async ValueTask Flush() => await this.writer.FlushAsync(this.cancellationToken).ConfigureAwait(false);
    }

    // Calls the handler without awaiting it, so this stays a small synchronous instantiation.
    private static bool TryDecode<TMessage>(ref ReadOnlySequence<byte> body, IHandler<TMessage> messageHandler,
        out ValueTask pending)
        where TMessage : struct, IMessage<TMessage>, allows ref struct
    {
        var primitiveReader = new PrimitiveReader(body);
        if (!TMessage.TryRead(ref primitiveReader, out var message, out _))
        {
            pending = default;
            return false;
        }

        body = primitiveReader.Remaining;
        pending = messageHandler.Handle(in message);
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
                ValueTask pending;

                var decoded = typeByte switch
                {
                    10 => TryDecode<ScaleMessage00>(ref body, this.handler, out pending),
                    11 => TryDecode<ScaleMessage01>(ref body, this.handler, out pending),
                    12 => TryDecode<ScaleMessage02>(ref body, this.handler, out pending),
                    13 => TryDecode<ScaleMessage03>(ref body, this.handler, out pending),
                    14 => TryDecode<ScaleMessage04>(ref body, this.handler, out pending),
                    15 => TryDecode<ScaleMessage05>(ref body, this.handler, out pending),
                    16 => TryDecode<ScaleMessage06>(ref body, this.handler, out pending),
                    17 => TryDecode<ScaleMessage07>(ref body, this.handler, out pending),
                    18 => TryDecode<ScaleMessage08>(ref body, this.handler, out pending),
                    19 => TryDecode<ScaleMessage09>(ref body, this.handler, out pending),
                    20 => TryDecode<ScaleMessage10>(ref body, this.handler, out pending),
                    21 => TryDecode<ScaleMessage11>(ref body, this.handler, out pending),
                    22 => TryDecode<ScaleMessage12>(ref body, this.handler, out pending),
                    23 => TryDecode<ScaleMessage13>(ref body, this.handler, out pending),
                    24 => TryDecode<ScaleMessage14>(ref body, this.handler, out pending),
                    25 => TryDecode<ScaleMessage15>(ref body, this.handler, out pending),
                    26 => TryDecode<ScaleMessage16>(ref body, this.handler, out pending),
                    27 => TryDecode<ScaleMessage17>(ref body, this.handler, out pending),
                    28 => TryDecode<ScaleMessage18>(ref body, this.handler, out pending),
                    29 => TryDecode<ScaleMessage19>(ref body, this.handler, out pending),
                    30 => TryDecode<ScaleMessage20>(ref body, this.handler, out pending),
                    31 => TryDecode<ScaleMessage21>(ref body, this.handler, out pending),
                    32 => TryDecode<ScaleMessage22>(ref body, this.handler, out pending),
                    33 => TryDecode<ScaleMessage23>(ref body, this.handler, out pending),
                    34 => TryDecode<ScaleMessage24>(ref body, this.handler, out pending),
                    35 => TryDecode<ScaleMessage25>(ref body, this.handler, out pending),
                    36 => TryDecode<ScaleMessage26>(ref body, this.handler, out pending),
                    37 => TryDecode<ScaleMessage27>(ref body, this.handler, out pending),
                    38 => TryDecode<ScaleMessage28>(ref body, this.handler, out pending),
                    39 => TryDecode<ScaleMessage29>(ref body, this.handler, out pending),
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

                if (!pending.IsCompleted)
                {
                    await pending.ConfigureAwait(false);
                }

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
