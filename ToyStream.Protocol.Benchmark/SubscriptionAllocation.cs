namespace ToyStream.Protocol.Benchmark;

using System.Buffers;
using System.Diagnostics;
using System.Diagnostics.CodeAnalysis;
using System.IO.Pipelines;
using Microsoft.Extensions.DependencyInjection;
using ToyStream.Client;
using ToyStream.Protocol.Messages;
using ToyStream.Server;
using ToyStream.Server.InMemory;
using ToyStream.Shared;

/// <summary>
/// Steady state allocation on the push path, measured directly rather than through BenchmarkDotNet:
/// allocation is deterministic, so it needs a counter and a loop, not a statistical engine.
/// </summary>
/// <remarks>
/// Run with <c>alloc</c>. Storage is a stand-in that hands back the same record forever, because a
/// real log allocates to retain what it is given and doubles its backing array as it grows - which
/// swamps the protocol path by two orders of magnitude and says nothing about it.
/// </remarks>
internal static class SubscriptionAllocation
{
    private const int Warmup = 20_000;
    private const int Measured = 100_000;

    public static async Task RunAsync()
    {
        Console.WriteLine($"Steady state push, {Measured:N0} records measured.");
        Console.WriteLine("Storage is a stand-in, so what is counted is the pump, the wire and the client.");
        Console.WriteLine();

        Console.WriteLine("  by batch size - fit B/record = perRecord + perBatch/batch to separate the two");
        await Measure("1 per batch", batch: 1, grouped: false, autoCommit: false);
        await Measure("10 per batch", batch: 10, grouped: false, autoCommit: false);
        await Measure("100 per batch", batch: 100, grouped: false, autoCommit: false);
        await Measure("1000 per batch", batch: 1000, grouped: false, autoCommit: false);

        Console.WriteLine();
        Console.WriteLine("  what a consumer group costs, at a realistic batch size");
        await Measure("no group", batch: 100, grouped: false, autoCommit: false);
        await Measure("group", batch: 100, grouped: true, autoCommit: false);
        await Measure("group, autocommit", batch: 100, grouped: true, autoCommit: true);

        Console.WriteLine();
        Console.WriteLine("  by record size, to tell framing overhead from payload handling");
        await Measure("empty", batch: 100, grouped: false, autoCommit: false, body: 0, key: 0);
        await Measure("21 B body", batch: 100, grouped: false, autoCommit: false, body: 21, key: 1);
        await Measure("4 KB body", batch: 100, grouped: false, autoCommit: false, body: 4096, key: 1);

        Console.WriteLine();
        Console.WriteLine("  by layer, to find which half of the stack it is in");
        await MeasureCodecOnly();

        Console.WriteLine();
        Console.WriteLine("  the probe's own cost, to be subtracted from the above");
        MeasureAppendsOnly();
    }

    /// <summary>
    /// The same pump and wire, but the client is a bare connection draining with
    /// <see cref="FetchedRecordReader"/> - no cursor, no batch handoff, no commits.
    /// </summary>
    private static async Task MeasureCodecOnly()
    {
        const int Chunk = 1_000;

        var partition = new ProbePartition();
        var services = new ServiceCollection()
            .AddSingleton<ITopicService>(new ProbeTopicService(partition))
            .AddSingleton<IConsumerGroupService, InMemoryConsumerGroupService>()
            .AddToyStreamServer()
            .BuildServiceProvider();

        using var cancellation = new CancellationTokenSource();
        var drain = new Drain();
        await Connect(services, drain, cancellation.Token);

        var delivered = 0L;
        for (var sent = 0; sent < Warmup; sent += Chunk)
        {
            delivered += Chunk;
            for (var i = 0; i < Chunk; i++)
            {
                partition.Append();
            }

            await drain.Reaching(delivered);
        }

        var started = Stopwatch.GetTimestamp();
        var before = GC.GetTotalAllocatedBytes(precise: true);
        for (var sent = 0; sent < Measured; sent += Chunk)
        {
            delivered += Chunk;
            for (var i = 0; i < Chunk; i++)
            {
                partition.Append();
            }

            await drain.Reaching(delivered);
        }

        var allocated = GC.GetTotalAllocatedBytes(precise: true) - before;
        var elapsed = Stopwatch.GetElapsedTime(started);

        await cancellation.CancelAsync();

        Console.WriteLine($"    {"codec only",-20} {(double)allocated / Measured,7:F1} B/record" +
                          $"   {elapsed.TotalMicroseconds / Measured,6:F2} us/record");
    }

    private static async Task Connect(IServiceProvider services, Drain drain, CancellationToken cancellationToken)
    {
        var options = new PipeOptions(
            readerScheduler: PipeScheduler.Inline,
            writerScheduler: PipeScheduler.Inline,
            useSynchronizationContext: false);

        var clientToServer = new Pipe(options);
        var serverToClient = new Pipe(options);

        _ = services.GetRequiredService<ConnectionAcceptor>()
            .Accept(new PipePair(clientToServer.Reader, serverToClient.Writer), cancellationToken);

        var client = new Connection<DrainHandler>(
            serverToClient.Reader, clientToServer.Writer, new DrainHandler(drain));
        _ = client.Start(cancellationToken);

        await ProtocolHandshake.WriteAsync(clientToServer.Writer, ConnectionRole.Subscriber, cancellationToken);
        await SendSubscribe(client, cancellationToken);
    }

    // Split out because a ref struct message cannot be built inside an async method.
    private static ValueTask SendSubscribe(ConnectionBase connection, CancellationToken cancellationToken) =>
        connection.SendMessage(
            new SubscribeMessage
            {
                Topic = "events",
                Group = string.Empty,
                MemberId = string.Empty,
                StartOffset = SubscribeOffset.Earliest
            },
            cancellationToken);

    private sealed class Drain
    {
        private TaskCompletionSource reached = new(TaskCreationOptions.RunContinuationsAsynchronously);
        private long count;
        private long target = long.MaxValue;

        public Task Reaching(long records)
        {
            lock (this)
            {
                if (Interlocked.Read(ref this.count) >= records)
                {
                    return Task.CompletedTask;
                }

                this.target = records;
                this.reached = new TaskCompletionSource(TaskCreationOptions.RunContinuationsAsynchronously);
                return this.reached.Task;
            }
        }

        public async ValueTask Read(PipeReader body, CancellationToken cancellationToken)
        {
            var records = new FetchedRecordReader(body);
            while (await records.MoveNextAsync(cancellationToken).ConfigureAwait(false))
            {
                if (Interlocked.Increment(ref this.count) >= Volatile.Read(ref this.target))
                {
                    lock (this)
                    {
                        this.reached.TrySetResult();
                    }
                }
            }
        }
    }

    private readonly struct DrainHandler(Drain drain) :
        IConnectionHandler,
        IStreamingRequestHandler<PushRecordsMessage>,
        IRequestHandler<SubscriptionLostMessage>
    {
        public DispatchResult TryParseAndDispatch(MessageType messageType, ref Dispatcher dispatch) =>
            messageType switch
            {
                MessageType.PushRecords => dispatch.StreamingRequest<DrainHandler, PushRecordsMessage>(this),
                MessageType.SubscriptionLost => dispatch.Request<DrainHandler, SubscriptionLostMessage>(this),
                _ => dispatch.Unexpected(messageType)
            };

        public ValueTask DispatchRequest(StreamingMessage<PushRecordsMessage> message,
            CancellationToken cancellationToken) => drain.Read(message.Body, cancellationToken);

        public ValueTask DispatchRequest(SubscriptionLostMessage message, CancellationToken cancellationToken) =>
            default;
    }

    /// <summary>The stand-in swaps a completion source per append, which is not the product's cost.</summary>
    private static void MeasureAppendsOnly()
    {
        var partition = new ProbePartition();
        for (var i = 0; i < Warmup; i++)
        {
            partition.Append();
        }

        var before = GC.GetTotalAllocatedBytes(precise: true);
        for (var i = 0; i < Measured; i++)
        {
            partition.Append();
        }

        var allocated = GC.GetTotalAllocatedBytes(precise: true) - before;
        Console.WriteLine($"    {"appends only",-20} {(double)allocated / Measured,7:F1} B/record");
    }

    private static async Task Measure(string label, int batch, bool grouped, bool autoCommit,
        int body = 21, int key = 1)
    {
        await using var rig = await Rig.Create(grouped, autoCommit, body, key);

        // Settles the JIT and the pipe's segment pool, so what follows is steady state.
        await rig.Run(Warmup, batch);

        var started = Stopwatch.GetTimestamp();
        var waitsBefore = rig.Waits;
        var before = GC.GetTotalAllocatedBytes(precise: true);
        await rig.Run(Measured, batch);
        var allocated = GC.GetTotalAllocatedBytes(precise: true) - before;
        var elapsed = Stopwatch.GetElapsedTime(started);
        var waits = rig.Waits - waitsBefore;

        Console.WriteLine($"    {label,-20} {(double)allocated / Measured,7:F1} B/record" +
                          $"   {elapsed.TotalMicroseconds / Measured,6:F2} us/record" +
                          $"   {(double)waits / Measured,6:F3} pump waits/record");
    }

    private sealed class Rig : IAsyncDisposable
    {
        private readonly CancellationTokenSource cancellation = new();

        private ProbePartition partition = null!;
        private ToyStreamSubscriber subscriber = null!;

        /// <summary>How many times the pump parked, which is the cost a batch of one cannot share.</summary>
        public long Waits => this.partition.Waits;

        public static async Task<Rig> Create(bool grouped, bool autoCommit, int body = 21, int key = 1)
        {
            var rig = new Rig { partition = new ProbePartition(body, key) };
            var services = new ServiceCollection()
                .AddSingleton<ITopicService>(new ProbeTopicService(rig.partition))
                .AddSingleton<IConsumerGroupService, InMemoryConsumerGroupService>()
                .AddToyStreamServer()
                .BuildServiceProvider();

            rig.subscriber = new ToyStreamSubscriber(
                new ProbeTransport(services, rig.cancellation.Token),
                new SubscriberOptions
                {
                    Topic = "events",
                    Group = grouped ? "readers" : string.Empty,
                    MemberId = grouped ? "a" : string.Empty,
                    AutoCommit = autoCommit
                });

            await rig.subscriber.ConnectAsync();
            return rig;
        }

        /// <summary>
        /// A batch of one charges every per-batch cost to a single record, which is the worst case
        /// for anything the group added at a batch boundary. A larger batch is the loaded shape.
        /// </summary>
        public async Task Run(int records, int batch)
        {
            for (var sent = 0; sent < records; sent += batch)
            {
                for (var i = 0; i < batch; i++)
                {
                    this.partition.Append();
                }

                for (var i = 0; i < batch; i++)
                {
                    await this.subscriber.MoveNextAsync();
                }
            }
        }

        public async ValueTask DisposeAsync()
        {
            await this.subscriber.DisposeAsync();
            await this.cancellation.CancelAsync();
            this.cancellation.Dispose();
        }
    }

    /// <summary>
    /// Hands back the same record at every offset and never stores anything, so a record costs the
    /// probe nothing and the only allocation left is the path under test.
    /// </summary>
    private sealed class ProbePartition(int bodySize = 21, int keySize = 1) : ITopicPartition
    {
        private readonly byte[] key = new byte[keySize];
        private readonly byte[] body = new byte[bodySize];

        private TaskCompletionSource appended = new(TaskCreationOptions.RunContinuationsAsynchronously);
        private long available;
        private long waits;

        /// <summary>One per <c>WaitForWork</c>, so it counts how often the pump had to park.</summary>
        public long Waits => Interlocked.Read(ref this.waits);

        /// <summary>Swapped before the old one completes, so a waiter always holds a live signal.</summary>
        public void Append()
        {
            Interlocked.Increment(ref this.available);
            Interlocked
                .Exchange(ref this.appended,
                    new TaskCompletionSource(TaskCreationOptions.RunContinuationsAsynchronously))
                .TrySetResult();
        }

        public PartitionDescription Describe() => new(0, Volatile.Read(ref this.available));

        public bool TryReadRecord(short topicIndex, short partitionId, long offset, out FetchedRecord record)
        {
            if (offset >= Volatile.Read(ref this.available))
            {
                record = default;
                return false;
            }

            record = new FetchedRecord(topicIndex, partitionId, offset, DateTime.UnixEpoch,
                new ReadOnlySequence<byte>(this.key), new ReadOnlySequence<byte>(this.body));
            return true;
        }

        /// <summary>
        /// Honours the token because the pump awaits every wait it registered once one of them wins,
        /// and a registration that can never complete would park the loop for good.
        /// </summary>
        public Task WaitForAppend(CancellationToken cancellationToken)
        {
            Interlocked.Increment(ref this.waits);

            var pending = Volatile.Read(ref this.appended).Task;
            return pending.IsCompleted ? pending : pending.WaitAsync(cancellationToken);
        }

        public IEventWriter GetEventWriter(RefBytes key, long? size) =>
            throw new NotSupportedException("The probe appends directly.");
    }

    private sealed class ProbeTopicService(ProbePartition partition) : ITopicService
    {
        private readonly ITopic topic = new ProbeTopic(partition);

        public ValueTask<bool> TryCreateTopic(RefUtf8String topicName, short partitionCount) => new(true);

        public bool TryGetTopic(RefUtf8String topicName, [NotNullWhen(true)] out ITopic? found)
        {
            found = this.topic;
            return true;
        }
    }

    private sealed class ProbeTopic(ProbePartition partition) : ITopic
    {
        public Utf8String Name { get; } = new("events");

        public int PartitionCount => 1;

        public bool TryGetPartition(short partitionId, [NotNullWhen(true)] out ITopicPartition? topicPartition)
        {
            topicPartition = partition;
            return partitionId == 0;
        }

        public void Describe(Span<PartitionDescription> destination) => destination[0] = partition.Describe();
    }

    /// <summary>Drives the real accept over paired pipes, so the measured path is the shipped one.</summary>
    private sealed class ProbeTransport(IServiceProvider services, CancellationToken cancellationToken)
        : IClientTransport
    {
        public ValueTask<ITransportConnection> ConnectAsync(CancellationToken token)
        {
            var options = new PipeOptions(
                readerScheduler: PipeScheduler.Inline,
                writerScheduler: PipeScheduler.Inline,
                useSynchronizationContext: false);

            var clientToServer = new Pipe(options);
            var serverToClient = new Pipe(options);

            _ = services.GetRequiredService<ConnectionAcceptor>()
                .Accept(new PipePair(clientToServer.Reader, serverToClient.Writer), cancellationToken);

            return new ValueTask<ITransportConnection>(
                new PipePair(serverToClient.Reader, clientToServer.Writer));
        }
    }

    private sealed class PipePair(PipeReader input, PipeWriter output) : ITransportConnection
    {
        public PipeReader Input => input;

        public PipeWriter Output => output;

        public async ValueTask DisposeAsync()
        {
            await input.CompleteAsync();
            await output.CompleteAsync();
        }
    }
}
