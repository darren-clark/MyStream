namespace ToyStream.Protocol.Benchmark.Allocation;

using System.IO.Pipelines;
using BenchmarkDotNet.Attributes;
using Messages;
using Protocol.Dispatch;
using Protocol.Dispatch.Context;
using Protocol.Dispatch.Direct;
using Responses;
using Shared;

/// <summary>
/// The three shapes still in contention, all already allocation free per round trip. What separates
/// them now is only how many distinct async methods a message passes through and how much of the
/// routing the JIT can see through.
/// </summary>
[MemoryDiagnoser]
public class AllocationBenchmarks
{
    private const int RoundTrips = 1000;

    private readonly List<IAsyncDisposable> disposables = [];

    private CancellationTokenSource cancellation = null!;
    private Utf8String topicName = null!;
    private DirectClientConnection directClient = null!;
    private MyDirectBenchmarkClient myDirectClient = null!;
    private ResponseTracker contextClient = null!;

    [GlobalSetup]
    public void Setup()
    {
        this.cancellation = new CancellationTokenSource();
        this.topicName = new Utf8String("test");

        var directPipes = CreatePipes();
        var directServer = new DirectServerConnection(
            directPipes.ClientToServer.Reader, directPipes.ServerToClient.Writer,
            new DirectNoopTopicService(), this.cancellation.Token);
        this.directClient = new DirectClientConnection(
            directPipes.ServerToClient.Reader, directPipes.ClientToServer.Writer, this.cancellation.Token);

        this.disposables.Add(directServer);
        this.disposables.Add(this.directClient);

        var myDirectPipes = CreatePipes();
        var myDirectServer = new MyDirectServerConnection(
            myDirectPipes.ClientToServer.Reader, myDirectPipes.ServerToClient.Writer);
        this.myDirectClient = new MyDirectBenchmarkClient(
            myDirectPipes.ServerToClient.Reader, myDirectPipes.ClientToServer.Writer);

        // Start returns only when the read loop ends, so it is deliberately not awaited here.
        _ = myDirectServer.Start(this.cancellation.Token);
        _ = this.myDirectClient.Start(this.cancellation.Token);

        var contextPipes = CreatePipes();
        var contextServer = new ContextConnection<ServerContext>(
            contextPipes.ClientToServer.Reader, contextPipes.ServerToClient.Writer,
            new ServerContext(new ContextNoopTopicService()));

        this.contextClient = new ResponseTracker(contextPipes.ClientToServer.Writer);
        var contextClientConnection = new ContextConnection<ClientContext>(
            contextPipes.ServerToClient.Reader, contextPipes.ClientToServer.Writer,
            new ClientContext(this.contextClient));

        _ = contextServer.Start(this.cancellation.Token);
        _ = contextClientConnection.Start(this.cancellation.Token);
    }

    private static (Pipe ClientToServer, Pipe ServerToClient) CreatePipes()
    {
        var options = new PipeOptions(
            readerScheduler: PipeScheduler.Inline,
            writerScheduler: PipeScheduler.Inline,
            useSynchronizationContext: false);

        return (new Pipe(options), new Pipe(options));
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

    /// <summary>Per-role connection dispatching directly to its own non-virtual methods.</summary>
    [Benchmark(OperationsPerInvoke = RoundTrips, Baseline = true)]
    public async Task Direct()
    {
        for (var i = 0; i < RoundTrips; i++)
        {
            await this.directClient.CreateTopic(this.topicName, 1);
        }
    }

    /// <summary>One-line arms: the handler reaches the generic dispatch as a typed delegate.</summary>
    [Benchmark(OperationsPerInvoke = RoundTrips)]
    public async Task MyDirect()
    {
        for (var i = 0; i < RoundTrips; i++)
        {
            await this.myDirectClient.CreateTopic(this.topicName, 1);
        }
    }

    /// <summary>One shared read loop; routing lives in a struct context the JIT specialises per role.</summary>
    [Benchmark(OperationsPerInvoke = RoundTrips)]
    public async Task Context()
    {
        for (var i = 0; i < RoundTrips; i++)
        {
            await this.contextClient.SendMessage<CreateTopicMessage, AcknowledgeMessage, AcknowledgeResponse>(
                new CreateTopicMessage { TopicName = this.topicName, PartitionCount = 1 },
                default);
        }
    }

    private sealed class MyDirectBenchmarkClient(PipeReader reader, PipeWriter writer)
        : MyDirectClientConnection(reader, writer)
    {
        public ValueTask<AcknowledgeResponse> CreateTopic(RefUtf8String topicName, short partitionCount)
            => this.SendMessage<CreateTopicMessage, AcknowledgeMessage, AcknowledgeResponse>(
                new CreateTopicMessage { TopicName = topicName, PartitionCount = partitionCount },
                default);
    }

    private sealed class DirectNoopTopicService : Protocol.Dispatch.Direct.ITopicService
    {
        public ValueTask CreateTopic(RefUtf8String topicName, short partitionCount) => default;
    }

    private sealed class ContextNoopTopicService : Protocol.Dispatch.Context.ITopicService
    {
        public ValueTask CreateTopic(RefUtf8String topicName, short partitionCount) => default;
    }
}
