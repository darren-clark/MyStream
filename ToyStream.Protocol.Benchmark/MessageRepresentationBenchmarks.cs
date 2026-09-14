namespace ToyStream.Protocol.Benchmark;

using System.IO.Pipelines;
using BenchmarkDotNet.Attributes;
using Dispatch;
using Messages;
using Responses;
using Shared;

/// <summary>
/// Measures what the ref struct read path actually buys: the server either reads the message as a
/// view over the pipe buffer, or copies it onto the heap first. Everything else is identical.
/// </summary>
[MemoryDiagnoser]
public class MessageRepresentationBenchmarks
{
    // Amortises BenchmarkDotNet's per-invocation overhead; results are reported per round trip.
    private const int RoundTrips = 1000;

    private readonly List<IAsyncDisposable> disposables = [];

    private CancellationTokenSource cancellation = null!;
    private Utf8String topicName = null!;

    private ClientConnection refStructClient = null!;
    private ClientConnection heapClient = null!;

    /// <summary>Payload size is the whole point: zero copy scales with it, a copy does not.</summary>
    [Params(4, 256, 4096)]
    public int TopicNameLength;

    [GlobalSetup]
    public void Setup()
    {
        this.cancellation = new CancellationTokenSource();
        this.topicName = new Utf8String(new string('x', this.TopicNameLength));

        this.refStructClient = CreatePair(static (reader, writer, ct) =>
            new RefStructServerConnection(reader, writer, ct));

        this.heapClient = CreatePair(static (reader, writer, ct) =>
            new HeapServerConnection(reader, writer, ct));
    }

    private ClientConnection CreatePair(Func<PipeReader, PipeWriter, CancellationToken, IAsyncDisposable> serverFactory)
    {
        var options = new PipeOptions(
            readerScheduler: PipeScheduler.Inline,
            writerScheduler: PipeScheduler.Inline,
            useSynchronizationContext: false);

        var clientPipe = new Pipe(options);
        var serverPipe = new Pipe(options);

        var client = new ClientConnection(serverPipe.Reader, clientPipe.Writer, this.cancellation.Token);
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

    /// <summary>Message stays a view over the pipe buffer.</summary>
    [Benchmark(OperationsPerInvoke = RoundTrips, Baseline = true)]
    public async Task RefStructMessage()
    {
        for (var i = 0; i < RoundTrips; i++)
        {
            await this.refStructClient.CreateTopic(this.topicName, 1);
        }
    }

    /// <summary>Message payload is copied onto the heap before the handler sees it.</summary>
    [Benchmark(OperationsPerInvoke = RoundTrips)]
    public async Task HeapMessage()
    {
        for (var i = 0; i < RoundTrips; i++)
        {
            await this.heapClient.CreateTopic(this.topicName, 1);
        }
    }

    private sealed class RefStructServerConnection : VirtualConnection
    {
        public RefStructServerConnection(PipeReader reader, PipeWriter writer, CancellationToken cancellationToken)
            : base(reader, writer, cancellationToken)
        {
        }

        protected override ValueTask HandleCreateTopicMessage(ref readonly CreateTopicMessage message) => default;
    }

    private sealed class HeapServerConnection : HeapMessageConnection
    {
        public HeapServerConnection(PipeReader reader, PipeWriter writer, CancellationToken cancellationToken)
            : base(reader, writer, cancellationToken)
        {
        }

        protected override ValueTask HandleCreateTopicMessage(HeapCreateTopicMessage message) => default;
    }

    private sealed class ClientConnection : VirtualConnection
    {
        public ClientConnection(PipeReader reader, PipeWriter writer, CancellationToken cancellationToken)
            : base(reader, writer, cancellationToken)
        {
        }

        protected override ValueTask HandleCreateTopicMessage(ref readonly CreateTopicMessage message) => default;

        public ValueTask<AcknowledgeResponse> CreateTopic(RefUtf8String topicName, short partitionCount)
            => SendMessage<CreateTopicMessage, AcknowledgeMessage, AcknowledgeResponse>(
                new CreateTopicMessage
                {
                    TopicName = topicName,
                    PartitionCount = partitionCount
                },
                default);
    }
}
