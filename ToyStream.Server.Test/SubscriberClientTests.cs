namespace ToyStream.Server.Test;

using System.Collections.Concurrent;
using System.IO.Pipelines;
using System.Text;
using ToyStream.Client;
using ToyStream.Protocol;
using ToyStream.Protocol.Messages;
using ToyStream.Protocol.Responses;
using ToyStream.Server.InMemory;
using ToyStream.Shared;

/// <summary>
/// The client half end to end: a real subscriber over a real accept, so the batch handoff that
/// parks the read loop is exercised rather than described.
/// </summary>
public class SubscriberClientTests
{
    [Test]
    public async Task ReadsRecordsPushedBeforeAndAfterSubscribing()
    {
        var service = SubscriptionHarness.ServiceWithTopic("events", 1);
        await SubscriptionHarness.Publish(service, "events", 0, "k1", "first");

        await using var harness = new SubscriptionHarness(service);
        await using var subscriber = harness.Subscriber("events");
        await subscriber.ConnectAsync();

        Assert.That(await Read(subscriber), Is.EqualTo("first"));

        await SubscriptionHarness.Publish(service, "events", 0, "k2", "second");

        Assert.That(await Read(subscriber), Is.EqualTo("second"));
    }

    /// <summary>Records keep arriving across batch boundaries, which is where the handoff lives.</summary>
    [Test]
    public async Task ReadsAcrossBatches()
    {
        var service = SubscriptionHarness.ServiceWithTopic("events", 1);

        await using var harness = new SubscriptionHarness(service);
        await using var subscriber = harness.Subscriber("events");
        await subscriber.ConnectAsync();

        var read = new List<string>();
        for (var i = 0; i < 10; i++)
        {
            await SubscriptionHarness.Publish(service, "events", 0, $"k{i}", $"body-{i}");
            read.Add(await Read(subscriber));
        }

        Assert.That(read, Is.EqualTo(Enumerable.Range(0, 10).Select(i => $"body-{i}")));
    }

    [Test]
    public async Task TheKeyAndOffsetTravelWithTheRecord()
    {
        var service = SubscriptionHarness.ServiceWithTopic("events", 1);
        await SubscriptionHarness.Publish(service, "events", 0, "the-key", "payload");

        await using var harness = new SubscriptionHarness(service);
        await using var subscriber = harness.Subscriber("events");

        Assert.That(await subscriber.MoveNextAsync(), Is.True);
        Assert.That(Encoding.UTF8.GetString(subscriber.Key.Span), Is.EqualTo("the-key"));
        Assert.That(subscriber.Current.Offset, Is.EqualTo(0));
        Assert.That(subscriber.Current.PartitionId, Is.EqualTo(0));
    }

    /// <summary>
    /// Subscribe has no answer, so a refusal has to reach the caller through the same channel the
    /// records would have used.
    /// </summary>
    [Test]
    public async Task AnUnknownTopicSurfacesAsSubscriptionLost()
    {
        await using var harness = new SubscriptionHarness(new InMemoryTopicService());
        await using var subscriber = harness.Subscriber("missing");

        var lost = Assert.ThrowsAsync<SubscriptionLostException>(
            async () => await subscriber.MoveNextAsync().AsTask().WaitAsync(TimeSpan.FromSeconds(10)));

        Assert.That(lost!.Reason, Is.EqualTo(SubscriptionLostReason.UnknownTopic));
    }

    /// <summary>A record skipped rather than read must not desynchronise the stream behind it.</summary>
    [Test]
    public async Task SkippingARecordBodyStillAdvances()
    {
        var service = SubscriptionHarness.ServiceWithTopic("events", 1);
        await SubscriptionHarness.Publish(service, "events", 0, "k1", "skipped");
        await SubscriptionHarness.Publish(service, "events", 0, "k2", "read");

        await using var harness = new SubscriptionHarness(service);
        await using var subscriber = harness.Subscriber("events");

        Assert.That(await subscriber.MoveNextAsync(), Is.True);
        Assert.That(await Read(subscriber), Is.EqualTo("read"));
    }

    /// <summary>
    /// Asking for the next record is what marks the previous one done, so a group that stops reading
    /// resumes behind the record it never got to rather than past it.
    /// </summary>
    [Test]
    public async Task AGroupResumesFromWhereItCommitted()
    {
        var service = SubscriptionHarness.ServiceWithTopic("events", 1);
        for (var i = 0; i < 4; i++)
        {
            await SubscriptionHarness.Publish(service, "events", 0, $"k{i}", $"body-{i}");
        }

        await using var harness = new SubscriptionHarness(service);
        var options = new SubscriberOptions { Group = "readers", StartAt = SubscribeOffset.Committed };

        var first = harness.Subscriber("events", options);
        Assert.That(await Read(first), Is.EqualTo("body-0"));
        Assert.That(await Read(first), Is.EqualTo("body-1"));

        // The checkpoint owed for body-1 only goes out on the way out.
        await first.DisposeAsync();
        await harness.WaitForCommit("readers", "events", partition: 0, offset: 2);

        var resumed = harness.Subscriber("events", options);
        Assert.That(await Read(resumed), Is.EqualTo("body-2"));
    }

    /// <summary>A subscriber with no group has nowhere to resume from, so it never starts.</summary>
    [Test]
    public void ResumingWithoutAGroupIsRejected()
    {
        Assert.Throws<ArgumentException>(() => _ = new ToyStreamSubscriber(
            new ThrowingTransport(),
            new SubscriberOptions { Topic = "events", StartAt = SubscribeOffset.Committed }));
    }

    /// <summary>
    /// A joining subscriber takes its share, and takes it from where the previous owner had got to
    /// rather than from the start of the partition.
    /// </summary>
    [Test]
    public async Task ASecondSubscriberTakesHalfThePartitions()
    {
        var service = SubscriptionHarness.ServiceWithTopic("events", 2);
        await using var harness = new SubscriptionHarness(service);

        var first = harness.Subscriber("events", Grouped("a"));
        using var reading = new Readers();
        var seenByFirst = reading.Start(first);

        await SubscriptionHarness.Publish(service, "events", 0, "k", "p0-first");
        await SubscriptionHarness.Publish(service, "events", 1, "k", "p1-first");
        await WaitUntil(() => seenByFirst.Count == 2, "the first subscriber never read both partitions");

        // Sorted by member id, so partition 1 belongs to "b" and has to be handed over.
        var second = harness.Subscriber("events", Grouped("b"));
        var seenBySecond = reading.Start(second);
        await WaitUntil(() => harness.OwnerOf(1) == "b",
            () => $"partition 1 was never handed over (p0={harness.OwnerOf(0) ?? "<none>"}, " +
                  $"p1={harness.OwnerOf(1) ?? "<none>"}); server: {harness.ServerFault() ?? "healthy"}", reading);

        await SubscriptionHarness.Publish(service, "events", 1, "k", "p1-second");
        await WaitUntil(() => seenBySecond.Has(1, 1), "partition 1 never reached the second subscriber");

        Assert.Multiple(() =>
        {
            Assert.That(seenBySecond.Has(1, 0), Is.False,
                "the handover replayed a record the first subscriber had already committed");
            Assert.That(seenByFirst.Has(1, 1), Is.False,
                "the first subscriber kept reading a partition it had given up");
        });
    }

    /// <summary>A member leaving has nothing to drain, so its partitions come back immediately.</summary>
    [Test]
    public async Task PartitionsReturnWhenASubscriberLeaves()
    {
        var service = SubscriptionHarness.ServiceWithTopic("events", 2);
        await using var harness = new SubscriptionHarness(service);

        using var reading = new Readers();
        var first = harness.Subscriber("events", Grouped("a"));
        var seenByFirst = reading.Start(first);

        var second = harness.Subscriber("events", Grouped("b"));
        var seenBySecond = reading.Start(second);
        await WaitUntil(() => harness.OwnerOf(1) == "b", "the second subscriber never took partition 1");

        await SubscriptionHarness.Publish(service, "events", 1, "k", "before");
        await WaitUntil(() => seenBySecond.Has(1, 0), "the second subscriber never read partition 1");

        await second.DisposeAsync();
        await WaitUntil(() => harness.OwnerOf(1) == "a", "partition 1 never came back");

        await SubscriptionHarness.Publish(service, "events", 1, "k", "after");
        await WaitUntil(() => seenByFirst.Has(1, 1), "the returned partition never resumed");
    }

    /// <summary>
    /// A subscriber that never reads cannot answer, so the drain has to expire rather than hold the
    /// partition for as long as it stays quiet.
    /// </summary>
    [Test]
    public async Task ASilentSubscriberDoesNotHoldThePartition()
    {
        var service = SubscriptionHarness.ServiceWithTopic("events", 2);
        await using var harness = new SubscriptionHarness(service, drainTimeout: TimeSpan.FromMilliseconds(250));

        // Connects and then never moves, so its read loop stays parked and no revoke is ever answered.
        var silent = harness.Subscriber("events", Grouped("a"));
        await silent.ConnectAsync();

        await SubscriptionHarness.Publish(service, "events", 1, "k", "stranded");

        using var reading = new Readers();
        var second = harness.Subscriber("events", Grouped("b"));
        var seenBySecond = reading.Start(second);

        await WaitUntil(() => harness.OwnerOf(1) == "b", "the drain never expired");
        await WaitUntil(() => seenBySecond.Has(1, 0), "the stranded record never reached the new owner");
    }

    /// <summary>
    /// A handover with records still flowing. The revoke sits behind every record it covers, so the
    /// departing subscriber commits what it was handed before it answers and the new owner resumes
    /// exactly there - neither a gap nor a replay, even though nothing paused.
    /// </summary>
    [Test]
    public async Task ARebalanceUnderLoadDeliversEveryRecordExactlyOnce()
    {
        const int perPartition = 200;
        const int joinAfter = 50;

        var service = SubscriptionHarness.ServiceWithTopic("events", 2);
        await using var harness = new SubscriptionHarness(service);
        using var reading = new Readers();

        var first = harness.Subscriber("events", Grouped("a"));
        var seenByFirst = reading.Start(first);
        await WaitUntil(() => harness.OwnerOf(0) == "a" && harness.OwnerOf(1) == "a",
            "the first subscriber never took the topic", reading);

        SeenRecords? seenBySecond = null;
        for (var i = 0; i < perPartition; i++)
        {
            await SubscriptionHarness.Publish(service, "events", 0, "k", $"p0-{i}");
            await SubscriptionHarness.Publish(service, "events", 1, "k", $"p1-{i}");

            // Mid-flight on purpose: the point is that the handover happens while records move.
            if (i == joinAfter)
            {
                seenBySecond = reading.Start(harness.Subscriber("events", Grouped("b")));
            }
        }

        await WaitUntil(() => harness.OwnerOf(1) == "b", "the rebalance never settled", reading);
        await WaitUntil(() => Delivered(seenByFirst, seenBySecond!).Length == perPartition * 2,
            () => $"only {Delivered(seenByFirst, seenBySecond!).Length} of {perPartition * 2} records arrived",
            reading);

        var delivered = Delivered(seenByFirst, seenBySecond!);
        Assert.Multiple(() =>
        {
            Assert.That(seenByFirst.Snapshot().Intersect(seenBySecond!.Snapshot()), Is.Empty,
                "a record was delivered to both subscribers");
            Assert.That(seenByFirst.Snapshot().Length + seenBySecond!.Snapshot().Length,
                Is.EqualTo(delivered.Length), "a record was delivered twice to the same subscriber");
            Assert.That(delivered.Select(record => record.Offset).Distinct().Count(),
                Is.EqualTo(perPartition), "the offsets delivered were not contiguous");
        });
    }

    /// <summary>
    /// A subscriber that connects and drops repeatedly. Each flap revokes a partition that is then
    /// never collected, so the group has to converge on the member still there rather than leave it
    /// held by one that has gone.
    /// </summary>
    [Test]
    public async Task AFlappingMemberDoesNotStrandAPartition()
    {
        const int flaps = 10;

        var service = SubscriptionHarness.ServiceWithTopic("events", 2);
        await using var harness = new SubscriptionHarness(service);
        using var reading = new Readers();

        var steady = harness.Subscriber("events", Grouped("a"));
        var seenBySteady = reading.Start(steady);
        await WaitUntil(() => harness.OwnerOf(0) == "a" && harness.OwnerOf(1) == "a",
            "the steady subscriber never took the topic", reading);

        for (var flap = 0; flap < flaps; flap++)
        {
            // Partition 0 belongs to "a" at every membership, so nothing here should touch it.
            await SubscriptionHarness.Publish(service, "events", 0, "k", $"p0-{flap}");

            var flapping = harness.Subscriber("events", Grouped("b"));
            await flapping.ConnectAsync();

            // Dropped only once the group has reacted, so the member always goes mid-handover
            // rather than racing past before a revoke was ever raised.
            await WaitUntil(() => harness.OwnerOf(1) != "a", $"flap {flap} never reached the group", reading);
            await flapping.DisposeAsync();

            await WaitUntil(() => harness.OwnerOf(1) == "a", $"flap {flap} left partition 1 stranded", reading);
        }

        await WaitUntil(() => harness.OwnerOf(0) == "a" && harness.OwnerOf(1) == "a",
            () => $"the group never settled (p0={harness.OwnerOf(0) ?? "<none>"}, " +
                  $"p1={harness.OwnerOf(1) ?? "<none>"}); server: {harness.ServerFault() ?? "healthy"}", reading);

        await SubscriptionHarness.Publish(service, "events", 1, "k", "after-the-churn");
        await WaitUntil(() => seenBySteady.Has(1, 0), "partition 1 was stranded by the churn", reading);

        var undisturbed = seenBySteady.Snapshot().Where(record => record.Partition == 0).ToArray();
        Assert.Multiple(() =>
        {
            Assert.That(undisturbed, Is.Unique, "the churn replayed a partition it never took");
            Assert.That(undisturbed, Has.Length.EqualTo(flaps), "the churn cost records on a settled partition");
        });
    }

    /// <summary>
    /// The flapping case closed: a connection that drops and comes straight back on the same id
    /// keeps its seat, so nothing is handed round and handed back in between.
    /// </summary>
    [Test]
    public async Task ReconnectingInsideTheLingerKeepsTheSameSeat()
    {
        var service = SubscriptionHarness.ServiceWithTopic("events", 2);
        await using var harness = new SubscriptionHarness(service, linger: TimeSpan.FromMinutes(1));
        using var reading = new Readers();

        var steady = harness.Subscriber("events", Grouped("a"));
        var seenBySteady = reading.Start(steady);

        var (flaky, drop) = harness.Droppable("events", Grouped("b"));
        reading.Start(flaky);
        await WaitUntil(() => harness.OwnerOf(1) == "b", "the flaky subscriber never took partition 1", reading);

        await SubscriptionHarness.Publish(service, "events", 1, "k", "before-the-drop");
        await WaitUntil(() => harness.Committed("readers", "events", 1) is not null,
            "the flaky subscriber never got going", reading);

        drop();

        var returning = harness.Subscriber("events", Grouped("b"));
        var seenByReturning = reading.Start(returning);
        await returning.ConnectAsync();

        await SubscriptionHarness.Publish(service, "events", 1, "k", "after-the-drop");
        await WaitUntil(() => seenByReturning.Has(1, 1), "the returning subscriber never got its partition back",
            reading);

        Assert.Multiple(() =>
        {
            Assert.That(harness.OwnerOf(1), Is.EqualTo("b"), "the seat changed hands across the reconnect");
            Assert.That(seenBySteady.Snapshot().Any(record => record.Partition == 1), Is.False,
                "partition 1 was handed round while its subscriber was reconnecting");
        });
    }

    /// <summary>A connection that does not come back is given up on, rather than held forever.</summary>
    [Test]
    public async Task ALostConnectionIsGivenUpOnceTheLingerExpires()
    {
        var service = SubscriptionHarness.ServiceWithTopic("events", 2);
        await using var harness = new SubscriptionHarness(service, linger: TimeSpan.FromMilliseconds(250));
        using var reading = new Readers();

        var steady = harness.Subscriber("events", Grouped("a"));
        var seenBySteady = reading.Start(steady);

        var (flaky, drop) = harness.Droppable("events", Grouped("b"));
        reading.Start(flaky);
        await WaitUntil(() => harness.OwnerOf(1) == "b", "the flaky subscriber never took partition 1", reading);

        drop();

        await WaitUntil(() => harness.OwnerOf(1) == "a", "the linger never expired", reading);

        await SubscriptionHarness.Publish(service, "events", 1, "k", "after-the-linger");
        await WaitUntil(() => seenBySteady.Has(1, 0), "partition 1 never resumed", reading);
    }

    private static (int Partition, long Offset)[] Delivered(params SeenRecords[] seen) =>
        seen.SelectMany(records => records.Snapshot()).Distinct().ToArray();

    private static SubscriberOptions Grouped(string memberId) =>
        new() { Group = "readers", MemberId = memberId, StartAt = SubscribeOffset.Committed };

    private static Task WaitUntil(Func<bool> condition, string because, Readers? readers = null) =>
        WaitUntil(condition, () => because, readers);

    private static async Task WaitUntil(Func<bool> condition, Func<string> because, Readers? readers = null)
    {
        var deadline = DateTime.UtcNow + TimeSpan.FromSeconds(10);
        while (DateTime.UtcNow < deadline)
        {
            if (condition())
            {
                return;
            }

            if (readers?.Faults.IsEmpty == false)
            {
                Assert.Fail($"{because()}; a subscriber failed: {string.Join(", ", readers.Faults)}");
            }

            await Task.Delay(10);
        }

        Assert.Fail(because());
    }

    private static async Task<string> Read(ToyStreamSubscriber subscriber)
    {
        Assert.That(await subscriber.MoveNextAsync().AsTask().WaitAsync(TimeSpan.FromSeconds(10)), Is.True);

        var payload = new Pipe();
        await subscriber.CopyBodyTo(payload.Writer);
        await payload.Writer.CompleteAsync();

        var read = await payload.Reader.ReadAsync();
        var body = Encoding.UTF8.GetString(read.Buffer);
        payload.Reader.AdvanceTo(read.Buffer.End);
        return body;
    }
}

/// <summary>Stands in where the subscriber is expected to fail before it ever connects.</summary>
internal sealed class ThrowingTransport : IClientTransport
{
    public ValueTask<ITransportConnection> ConnectAsync(CancellationToken cancellationToken) =>
        throw new InvalidOperationException("The subscriber should not have connected.");
}

/// <summary>What a subscriber has been handed, by partition and offset.</summary>
internal sealed class SeenRecords
{
    private readonly ConcurrentQueue<(int Partition, long Offset)> records = new();

    public int Count => this.records.Count;

    public bool Has(int partition, long offset) =>
        this.records.Any(record => record.Partition == partition && record.Offset == offset);

    public (int Partition, long Offset)[] Snapshot() => this.records.ToArray();

    internal void Add(short partition, long offset) => this.records.Enqueue((partition, offset));
}

/// <summary>
/// Drives subscribers the way an <c>await foreach</c> would, with a move always in flight. That is
/// what keeps a read loop free to see a revoke: a subscriber parked mid-batch cannot answer one.
/// </summary>
internal sealed class Readers : IDisposable
{
    private readonly CancellationTokenSource cancellation = new();
    private readonly List<Task> pumps = [];

    public SeenRecords Start(ToyStreamSubscriber subscriber)
    {
        var seen = new SeenRecords();
        this.pumps.Add(Task.Run(async () =>
        {
            try
            {
                while (await subscriber.MoveNextAsync(this.cancellation.Token))
                {
                    seen.Add(subscriber.Current.PartitionId, subscriber.Current.Offset);
                }
            }
            catch (Exception exception)
            {
                Faults.Add(exception);
            }
        }, this.cancellation.Token));

        return seen;
    }

    /// <summary>
    /// Kept rather than swallowed: a subscriber failing shows up as a wait that never completes,
    /// which says nothing about why.
    /// </summary>
    public System.Collections.Concurrent.ConcurrentBag<Exception> Faults { get; } = [];

    public void Dispose()
    {
        this.cancellation.Cancel();
        this.cancellation.Dispose();
    }
}

/// <summary>Runs the real accept over a pipe pair, so a client sees a genuine server.</summary>
internal sealed class SubscriptionHarness : IAsyncDisposable
{
    private readonly CancellationTokenSource cancellation = new();
    private readonly List<ToyStreamSubscriber> subscribers = [];
    private readonly List<Task> accepts = [];
    private readonly ITopicService service;

    /// <summary>Shared across connections, because a checkpoint outliving one is the whole point.</summary>
    private readonly IConsumerGroupService groups = new InMemoryConsumerGroupService();

    /// <summary>
    /// Shared for a stronger reason: membership is observed from the connections, so a coordinator
    /// per connection would put every subscriber in a group of one.
    /// </summary>
    private readonly ConsumerGroupCoordinator coordinator;

    public SubscriptionHarness(ITopicService service, TimeSpan? drainTimeout = null, TimeSpan? linger = null)
    {
        this.service = service;
        this.coordinator = new ConsumerGroupCoordinator(this.groups)
        {
            DrainTimeout = drainTimeout ?? TimeSpan.FromSeconds(30),
            MemberLinger = linger ?? TimeSpan.FromSeconds(5)
        };
    }

    public ToyStreamSubscriber Subscriber(string topic, SubscriberOptions? options = null)
    {
        var subscriber = new ToyStreamSubscriber(
            new Transport(this.service, this.groups, this.coordinator, this.accepts, this.cancellation.Token),
            (options ?? new SubscriberOptions()) with { Topic = topic });
        this.subscribers.Add(subscriber);
        return subscriber;
    }

    /// <summary>
    /// A subscriber whose connection can be cut from under it, which is the only way to reach the
    /// linger: disposing one properly makes it say it is leaving.
    /// </summary>
    public (ToyStreamSubscriber Subscriber, Action Drop) Droppable(string topic, SubscriberOptions options)
    {
        var transport = new Transport(this.service, this.groups, this.coordinator, this.accepts,
            this.cancellation.Token);
        var subscriber = new ToyStreamSubscriber(transport, options with { Topic = topic });
        this.subscribers.Add(subscriber);
        return (subscriber, transport.Drop);
    }

    /// <summary>
    /// The accept runs detached, so a server side fault otherwise shows up only as a client that
    /// stops being told anything.
    /// </summary>
    public string? ServerFault()
    {
        foreach (var accept in this.accepts.ToArray())
        {
            if (accept.IsFaulted)
            {
                return accept.Exception?.GetBaseException().ToString();
            }
        }

        return null;
    }

    public async ValueTask DisposeAsync()
    {
        foreach (var subscriber in this.subscribers)
        {
            await subscriber.DisposeAsync();
        }

        await this.cancellation.CancelAsync();
        this.cancellation.Dispose();
    }

    /// <summary>
    /// A commit is one way, so the only way to know the server has it is to watch for it. Polled
    /// rather than awaited because nothing on the client side is told.
    /// </summary>
    public async Task WaitForCommit(string group, string topic, short partition, long offset)    {
        var deadline = DateTime.UtcNow + TimeSpan.FromSeconds(10);
        while (DateTime.UtcNow < deadline)
        {
            if (this.groups.TryGetCommittedOffset(new Utf8String(group), new Utf8String(topic), partition,
                    out var committed) && committed == offset)
            {
                return;
            }

            await Task.Delay(10);
        }

        Assert.Fail($"{group} never committed {topic}/{partition} at {offset}.");
    }

    /// <summary>Settled ownership, so a test can wait for a handover instead of racing it.</summary>
    public string? OwnerOf(short partition, string group = "readers", string topic = "events")
    {
        var owner = this.coordinator.OwnerOf(new Utf8String(group), new Utf8String(topic), partition);
        return owner is null ? null : (string)owner;
    }

    public long? Committed(string group, string topic, short partition) =>
        this.groups.TryGetCommittedOffset(new Utf8String(group), new Utf8String(topic), partition, out var offset)
            ? offset
            : null;

    public static ITopicService ServiceWithTopic(string name, short partitions)
    {
        var service = new InMemoryTopicService();
        IRequestHandler<CreateTopicMessage, AcknowledgeMessage<CreateTopicResult>> handler =
            new CreateTopicHandler(service);

        var dispatched = handler.DispatchRequest(
            new CreateTopicMessage { TopicName = new Utf8String(name), PartitionCount = partitions },
            new ResponseWriter<AcknowledgeMessage<CreateTopicResult>>(new Pipe().Writer), default);

        Assert.That(dispatched.IsCompletedSuccessfully, Is.True);
        return service;
    }

    public static async Task Publish(ITopicService service, string topic, short partition, string key, string body)
    {
        var payload = new Pipe();
        await payload.Writer.WriteAsync(Encoding.UTF8.GetBytes(body));
        await payload.Writer.CompleteAsync();

        IStreamingRequestHandler<PublishMessage, AcknowledgeMessage<PublishResult>> handler =
            new PublishHandler(service);

        await handler.DispatchRequest(
            new StreamingMessage<PublishMessage>
            {
                Header = new PublishMessage
                {
                    Topic = new Utf8String(topic),
                    Partition = partition,
                    Key = new RefBytes(Encoding.UTF8.GetBytes(key))
                },
                Size = Encoding.UTF8.GetByteCount(body),
                Body = payload.Reader
            },
            new ResponseWriter<AcknowledgeMessage<PublishResult>>(new Pipe().Writer), default);
    }

    private sealed class Transport(ITopicService service, IConsumerGroupService groups,
        ConsumerGroupCoordinator coordinator, List<Task> accepts, CancellationToken cancellationToken)
        : IClientTransport
    {
        private PipeWriter? clientWrites;

        /// <summary>Completes the half the server reads, which is what a dead socket looks like to it.</summary>
        public void Drop() => this.clientWrites?.Complete();

        public ValueTask<ITransportConnection> ConnectAsync(CancellationToken token)
        {
            var clientToServer = new Pipe();
            var serverToClient = new Pipe();
            this.clientWrites = clientToServer.Writer;

            accepts.Add(TestServer.Serving(service, groups, coordinator)
                .Accept(new PipePair(clientToServer.Reader, serverToClient.Writer), cancellationToken));

            return new ValueTask<ITransportConnection>(
                new PipePair(serverToClient.Reader, clientToServer.Writer));
        }
    }
}
