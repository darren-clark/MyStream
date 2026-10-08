namespace ToyStream.Protocol.Benchmark.SwitchScale;

using System.Buffers;
using System.IO.Pipelines;
using System.Runtime.InteropServices;
using BenchmarkDotNet.Attributes;
using Messages;
using Protocol.Dispatch.Direct;
using Responses;
using Scale;

/// <summary>
/// Direct dispatch has ~8 line switch arms, so this asks what a wide switch costs: first compiling
/// in 30 arms but only using one (code size), then cycling all 30 (branch prediction and i-cache).
/// </summary>
[MemoryDiagnoser]
public class SwitchScaleBenchmarks
{
    private const int MessageTypes = 30;
    private const int Batches = 30;
    private const int Operations = MessageTypes * Batches;

    private readonly List<IAsyncDisposable> disposables = [];

    private CancellationTokenSource cancellation = null!;
    private ScaleDirectClient narrowClient = null!;
    private ScaleDirectClient wideClient = null!;
    private ScaleDirectClient handlerNarrowClient = null!;
    private ScaleDirectClient handlerWideClient = null!;

    [GlobalSetup]
    public void Setup()
    {
        this.cancellation = new CancellationTokenSource();

        this.narrowClient = CreatePair(static (reader, writer, ct) =>
            new ScaleDirectServerNarrow(reader, writer, ct));

        this.wideClient = CreatePair(static (reader, writer, ct) =>
            new ScaleDirectServerWide(reader, writer, ct));

        this.handlerNarrowClient = CreatePair(static (reader, writer, ct) =>
            new ScaleHandlerServerNarrow(reader, writer, new ScaleMessageHandler(), ct));

        this.handlerWideClient = CreatePair(static (reader, writer, ct) =>
            new ScaleHandlerServerWide(reader, writer, new ScaleMessageHandler(), ct));
    }

    private ScaleDirectClient CreatePair(Func<PipeReader, PipeWriter, CancellationToken, IAsyncDisposable> serverFactory)
    {
        var options = new PipeOptions(
            readerScheduler: PipeScheduler.Inline,
            writerScheduler: PipeScheduler.Inline,
            useSynchronizationContext: false);

        var clientPipe = new Pipe(options);
        var serverPipe = new Pipe(options);

        var client = new ScaleDirectClient(serverPipe.Reader, clientPipe.Writer, this.cancellation.Token);
        var server = serverFactory(clientPipe.Reader, serverPipe.Writer, this.cancellation.Token);

        this.disposables.Add(client);
        this.disposables.Add(server);
        return client;
    }

    [GlobalCleanup]
    public async Task Cleanup()
    {
        await this.cancellation.CancelAsync();
        foreach (var disposable in this.disposables)
        {
            await disposable.DisposeAsync();
        }

        this.cancellation.Dispose();
    }

    /// <summary>One arm compiled in, one type used. The floor.</summary>
    [Benchmark(OperationsPerInvoke = Operations, Baseline = true)]
    public async Task NarrowSwitch_OneType()
    {
        for (var i = 0; i < Operations; i++)
        {
            await this.narrowClient.Send(new ScaleMessage00 { Value = 1 });
        }
    }

    /// <summary>Thirty arms compiled in, still only one used: isolates code size.</summary>
    [Benchmark(OperationsPerInvoke = Operations)]
    public async Task WideSwitch_OneType()
    {
        for (var i = 0; i < Operations; i++)
        {
            await this.wideClient.Send(new ScaleMessage00 { Value = 1 });
        }
    }

    /// <summary>Thirty arms, all exercised: adds branch prediction and i-cache pressure.</summary>
    [Benchmark(OperationsPerInvoke = Operations)]
    public async Task WideSwitch_AllTypes()
    {
        for (var batch = 0; batch < Batches; batch++)
        {
            await SendAllTypes(this.wideClient);
        }
    }

    /// <summary>Handler shape, one arm compiled in, one type used.</summary>
    [Benchmark(OperationsPerInvoke = Operations)]
    public async Task HandlerNarrowSwitch_OneType()
    {
        for (var i = 0; i < Operations; i++)
        {
            await this.handlerNarrowClient.Send(new ScaleMessage00 { Value = 1 });
        }
    }

    /// <summary>Handler shape, thirty arms compiled in, one type used: its code size effect.</summary>
    [Benchmark(OperationsPerInvoke = Operations)]
    public async Task HandlerWideSwitch_OneType()
    {
        for (var i = 0; i < Operations; i++)
        {
            await this.handlerWideClient.Send(new ScaleMessage00 { Value = 1 });
        }
    }

    /// <summary>Handler shape, thirty arms, all exercised.</summary>
    [Benchmark(OperationsPerInvoke = Operations)]
    public async Task HandlerWideSwitch_AllTypes()
    {
        for (var batch = 0; batch < Batches; batch++)
        {
            await SendAllTypes(this.handlerWideClient);
        }
    }

    private static async Task SendAllTypes(ScaleDirectClient client)
    {
        await client.Send(new ScaleMessage00 { Value = 1 });
        await client.Send(new ScaleMessage01 { Value = 1 });
        await client.Send(new ScaleMessage02 { Value = 1 });
        await client.Send(new ScaleMessage03 { Value = 1 });
        await client.Send(new ScaleMessage04 { Value = 1 });
        await client.Send(new ScaleMessage05 { Value = 1 });
        await client.Send(new ScaleMessage06 { Value = 1 });
        await client.Send(new ScaleMessage07 { Value = 1 });
        await client.Send(new ScaleMessage08 { Value = 1 });
        await client.Send(new ScaleMessage09 { Value = 1 });
        await client.Send(new ScaleMessage10 { Value = 1 });
        await client.Send(new ScaleMessage11 { Value = 1 });
        await client.Send(new ScaleMessage12 { Value = 1 });
        await client.Send(new ScaleMessage13 { Value = 1 });
        await client.Send(new ScaleMessage14 { Value = 1 });
        await client.Send(new ScaleMessage15 { Value = 1 });
        await client.Send(new ScaleMessage16 { Value = 1 });
        await client.Send(new ScaleMessage17 { Value = 1 });
        await client.Send(new ScaleMessage18 { Value = 1 });
        await client.Send(new ScaleMessage19 { Value = 1 });
        await client.Send(new ScaleMessage20 { Value = 1 });
        await client.Send(new ScaleMessage21 { Value = 1 });
        await client.Send(new ScaleMessage22 { Value = 1 });
        await client.Send(new ScaleMessage23 { Value = 1 });
        await client.Send(new ScaleMessage24 { Value = 1 });
        await client.Send(new ScaleMessage25 { Value = 1 });
        await client.Send(new ScaleMessage26 { Value = 1 });
        await client.Send(new ScaleMessage27 { Value = 1 });
        await client.Send(new ScaleMessage28 { Value = 1 });
        await client.Send(new ScaleMessage29 { Value = 1 });
    }
}

/// <summary>Client role for the scale messages: sends requests, only ever receives acknowledgements.</summary>
public sealed class ScaleDirectClient : DirectConnection
{
    public ScaleDirectClient(PipeReader reader, PipeWriter writer, CancellationToken cancellationToken)
        : base(reader, writer, cancellationToken)
        => this.StartReading();

    public ValueTask<AcknowledgeResponse> Send<TMessage>(TMessage message)
        where TMessage : struct, IRequestMessage<TMessage, AcknowledgeMessage>, allows ref struct
        => this.SendMessage<TMessage, AcknowledgeMessage, AcknowledgeResponse>(message, default);

    protected override async Task Read()
    {
        await Task.Yield();
        var buffer = ReadOnlySequence<byte>.Empty;

        try
        {
            while (!this.CancellationToken.IsCancellationRequested)
            {
                if (buffer.Length < sizeof(MessageType))
                {
                    var result = await this.Reader.ReadAsync(this.CancellationToken).ConfigureAwait(false);
                    buffer = result.Buffer;
                }

                var messageType = (MessageType)MemoryMarshal.Read<byte>(buffer.Slice(0, 1).FirstSpan);
                var body = buffer.Slice(1);

                switch (messageType)
                {
                    case MessageType.Acknowledge:
                    {
                        var primitiveReader = new PrimitiveReader(body);
                        if (!TryDecode<AcknowledgeMessage>(ref primitiveReader, out var acknowledge))
                        {
                            buffer = await this.RefillAsync(buffer).ConfigureAwait(false);
                            continue;
                        }

                        body = primitiveReader.Remaining;
                        this.CompleteResponse<AcknowledgeMessage, AcknowledgeResponse>(in acknowledge);
                        break;
                    }

                    default:
                        throw new InvalidOperationException($"Client received unexpected message type {messageType}.");
                }

                buffer = body;
                this.Reader.AdvanceTo(buffer.Start);
            }
        }
        catch (OperationCanceledException)
        {
        }
    }
}
