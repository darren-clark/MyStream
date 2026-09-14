namespace ToyStream.Protocol.Benchmark;

using System.IO.Pipelines;
using BenchmarkDotNet.Attributes;
using Dispatch;
using Messages;
using Responses;
using Shared;

[MemoryDiagnoser]
[ThreadingDiagnoser]
public class Benchmarks
{
    // Amortises BenchmarkDotNet's per-invocation overhead; results are reported per round trip.
    private const int RoundTrips = 1000;

    private readonly List<IAsyncDisposable> disposables = [];

    private CancellationTokenSource cancellation = null!;
    private Utf8String topicName = null!;

    private ClientConnection virtualClient = null!;
    private ClientConnection genericHandlerClient = null!;
    private ClientConnection structHandlerClient = null!;

    [GlobalSetup]
    public void Setup()
    {
        this.cancellation = new CancellationTokenSource();
        this.topicName = new Utf8String("test");

        // The client is the same implementation in every arm; only the server's dispatch differs.
        this.virtualClient = CreatePair(static (reader, writer, ct) =>
            new VirtualServerConnection(reader, writer, ct));

        this.genericHandlerClient = CreatePair(static (reader, writer, ct) =>
            new GenericHandlerConnection(reader, writer, new ClassAckHandler(), ct));

        this.structHandlerClient = CreatePair(static (reader, writer, ct) =>
            new StructHandlerConnection<StructAckHandler>(reader, writer, default, ct));
    }

    private ClientConnection CreatePair(Func<PipeReader, PipeWriter, CancellationToken, IAsyncDisposable> serverFactory)
    {
        // Inline schedulers keep the round trip on one thread: without them the Pipe queues each
        // reader continuation to the thread pool, costing two handoffs per round trip.
        var options = new PipeOptions(
            readerScheduler: PipeScheduler.Inline,
            writerScheduler: PipeScheduler.Inline,
            useSynchronizationContext: false);

        // Client reads what the server writes, and writes what the server reads.
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

    /// <summary>Server dispatches through an abstract method override.</summary>
    [Benchmark(OperationsPerInvoke = RoundTrips, Baseline = true)]
    public async Task VirtualMethod()
    {
        for (var i = 0; i < RoundTrips; i++)
        {
            await this.virtualClient.CreateTopic(this.topicName, 1);
        }
    }

    /// <summary>Server dispatches through an injected interface, so the call stays virtual.</summary>
    [Benchmark(OperationsPerInvoke = RoundTrips)]
    public async Task GenericHandler()
    {
        for (var i = 0; i < RoundTrips; i++)
        {
            await this.genericHandlerClient.CreateTopic(this.topicName, 1);
        }
    }

    /// <summary>Server dispatches through a struct type argument, so the call should devirtualize.</summary>
    [Benchmark(OperationsPerInvoke = RoundTrips)]
    public async Task StructHandler()
    {
        for (var i = 0; i < RoundTrips; i++)
        {
            await this.structHandlerClient.CreateTopic(this.topicName, 1);
        }
    }

    private sealed class ClassAckHandler : IMessageHandler
    {
        public ValueTask HandleMessage<TRequestMessage, TResponseMessage>(
            ref readonly TRequestMessage message, IResponseSender sender)
            where TRequestMessage : struct, IRequestMessage<TRequestMessage, TResponseMessage>, allows ref struct
            where TResponseMessage : struct, IResponseMessage<TResponseMessage>, allows ref struct
            => sender.Send(default(TResponseMessage));
    }

    private readonly struct StructAckHandler : IMessageHandler
    {
        public ValueTask HandleMessage<TRequestMessage, TResponseMessage>(
            ref readonly TRequestMessage message, IResponseSender sender)
            where TRequestMessage : struct, IRequestMessage<TRequestMessage, TResponseMessage>, allows ref struct
            where TResponseMessage : struct, IResponseMessage<TResponseMessage>, allows ref struct
            => sender.Send(default(TResponseMessage));
    }

    // All we do is simply ack the request.
    private sealed class VirtualServerConnection : VirtualConnection
    {
        public VirtualServerConnection(PipeReader reader, PipeWriter writer, CancellationToken cancellationToken)
            : base(reader, writer, cancellationToken)
        {
        }

        protected override ValueTask HandleCreateTopicMessage(ref readonly CreateTopicMessage message)
            => Write(new AcknowledgeMessage(), default);
    }

    private sealed class ClientConnection : VirtualConnection
    {
        public ClientConnection(PipeReader reader, PipeWriter writer, CancellationToken cancellationToken)
            : base(reader, writer, cancellationToken)
        {
        }

        protected override ValueTask HandleCreateTopicMessage(ref readonly CreateTopicMessage message)
            => throw new NotImplementedException();

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
