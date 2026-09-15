namespace ToyStream.Protocol.Benchmark.Scale;

using System.IO.Pipelines;
using BenchmarkDotNet.Attributes;
using Dispatch;
using Messages;
using Responses;

/// <summary>
/// With one message type every read loop shape looks the same. This asks what changes at thirty:
/// whether an async instantiation per type costs anything, and whether cycling through many types
/// costs more than hammering one.
/// </summary>
[MemoryDiagnoser]
public class ScaleBenchmarks
{
    private const int MessageTypes = 30;
    private const int Batches = 30;
    private const int Operations = MessageTypes * Batches;

    private readonly List<IAsyncDisposable> disposables = [];

    private CancellationTokenSource cancellation = null!;
    private ScaleClient asyncPerTypeClient = null!;
    private ScaleClient syncFastPathClient = null!;

    [GlobalSetup]
    public void Setup()
    {
        this.cancellation = new CancellationTokenSource();

        this.asyncPerTypeClient = CreatePair(static (reader, writer, ct) =>
            new AsyncPerTypeConnection(reader, writer, ct));

        this.syncFastPathClient = CreatePair(static (reader, writer, ct) =>
            new SyncFastPathConnection(reader, writer, ct));
    }

    private ScaleClient CreatePair(Func<PipeReader, PipeWriter, CancellationToken, IAsyncDisposable> serverFactory)
    {
        var options = new PipeOptions(
            readerScheduler: PipeScheduler.Inline,
            writerScheduler: PipeScheduler.Inline,
            useSynchronizationContext: false);

        var clientPipe = new Pipe(options);
        var serverPipe = new Pipe(options);

        var client = new ScaleClient(serverPipe.Reader, clientPipe.Writer, this.cancellation.Token);
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

    [Benchmark(OperationsPerInvoke = Operations, Baseline = true)]
    public async Task AsyncPerType_AllTypes()
    {
        for (var batch = 0; batch < Batches; batch++)
        {
            await SendAllTypes(this.asyncPerTypeClient);
        }
    }

    [Benchmark(OperationsPerInvoke = Operations)]
    public async Task SyncFastPath_AllTypes()
    {
        for (var batch = 0; batch < Batches; batch++)
        {
            await SendAllTypes(this.syncFastPathClient);
        }
    }

    [Benchmark(OperationsPerInvoke = Operations)]
    public async Task AsyncPerType_SingleType()
    {
        for (var i = 0; i < Operations; i++)
        {
            await this.asyncPerTypeClient.Send(new ScaleMessage00 { Value = 1 });
        }
    }

    [Benchmark(OperationsPerInvoke = Operations)]
    public async Task SyncFastPath_SingleType()
    {
        for (var i = 0; i < Operations; i++)
        {
            await this.syncFastPathClient.Send(new ScaleMessage00 { Value = 1 });
        }
    }

    private static async Task SendAllTypes(ScaleClient client)
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

    private sealed class ScaleClient : VirtualConnection
    {
        public ScaleClient(PipeReader reader, PipeWriter writer, CancellationToken cancellationToken)
            : base(reader, writer, cancellationToken)
        {
        }

        protected override ValueTask HandleCreateTopicMessage(ref readonly CreateTopicMessage message) => default;

        public ValueTask<AcknowledgeResponse> Send<TMessage>(TMessage message)
            where TMessage : struct, IRequestMessage<TMessage, AcknowledgeMessage>, allows ref struct
            => SendMessage<TMessage, AcknowledgeMessage, AcknowledgeResponse>(message, default);
    }
}
