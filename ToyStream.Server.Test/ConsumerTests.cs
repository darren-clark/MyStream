namespace ToyStream.Server.Test;

using System.Buffers;
using System.IO.Pipelines;
using System.Text;
using ToyStream.Client;
using ToyStream.Protocol;
using ToyStream.Protocol.Responses;
using ToyStream.Server.InMemory;

/// <summary>
/// The consumer against a real server over an in-process transport: everything from describe
/// through streamed records is exercised, with no socket and no test double for the protocol.
/// </summary>
public class ConsumerTests
{
    [Test]
    public async Task ConsumesWhatWasPublished()
    {
        await using var cluster = new Cluster();
        await cluster.CreateTopic("events", 1);
        await cluster.Publish("events", 0, "k0", "first");
        await cluster.Publish("events", 0, "k1", "second");

        await using var consumer = cluster.Consumer(["events"]);

        Assert.That(await Drain(consumer), Is.EqualTo(new[] { ("k0", "first"), ("k1", "second") }));
    }

    /// <summary>The offsets move as records are handed out, so a second pass sees only what is new.</summary>
    [Test]
    public async Task OffsetsAdvanceAcrossFetches()
    {
        await using var cluster = new Cluster();
        await cluster.CreateTopic("events", 1);
        await cluster.Publish("events", 0, "a", "one");

        await using var consumer = cluster.Consumer(["events"]);
        Assert.That(await Drain(consumer), Is.EqualTo(new[] { ("a", "one") }));

        await cluster.Publish("events", 0, "b", "two");

        Assert.That(await Drain(consumer), Is.EqualTo(new[] { ("b", "two") }),
            "the consumer re-read what it had already handed out");
    }

    /// <summary>A record is identified by index into the table, so the name has to come back out of it.</summary>
    [Test]
    public async Task RecordsAreAttributedToTheirTopic()
    {
        await using var cluster = new Cluster();
        await cluster.CreateTopic("events", 1);
        await cluster.CreateTopic("audit", 1);
        await cluster.Publish("events", 0, "a", "from-events");
        await cluster.Publish("audit", 0, "b", "from-audit");

        await using var consumer = cluster.Consumer(["events", "audit"]);

        var seen = new List<(string Topic, string Body)>();
        while (await consumer.MoveNextAsync())
        {
            seen.Add(((string)consumer.Topic, await BodyText(consumer)));
        }

        Assert.That(seen, Is.EquivalentTo(new[] { ("events", "from-events"), ("audit", "from-audit") }));
    }

    [Test]
    public async Task StartingAtLatestSkipsWhatIsAlreadyThere()
    {
        await using var cluster = new Cluster();
        await cluster.CreateTopic("events", 1);
        await cluster.Publish("events", 0, "old", "before");

        await using var consumer = cluster.Consumer(["events"],
            new ConsumerOptions { StartAt = StartPosition.Latest, MaxWaitMilliseconds = 0 });
        await consumer.ConnectAsync();

        await cluster.Publish("events", 0, "new", "after");

        Assert.That(await Drain(consumer), Is.EqualTo(new[] { ("new", "after") }));
    }

    /// <summary>The whole point of streaming: a record far larger than a chunk arrives intact.</summary>
    [Test]
    public async Task ConsumesARecordLargerThanAChunk()
    {
        await using var cluster = new Cluster();
        await cluster.CreateTopic("events", 1);

        var large = new string('x', (ProtocolConstants.MaxChunkSize * 3) / 2);
        await cluster.Publish("events", 0, "big", large);
        await cluster.Publish("events", 0, "small", "after");

        await using var consumer = cluster.Consumer(["events"],
            new ConsumerOptions { MaxWaitMilliseconds = 0, MaxBytes = ProtocolConstants.MaxChunkSize * 4 });

        Assert.That(await consumer.MoveNextAsync(), Is.True);
        Assert.That(await BodyText(consumer), Has.Length.EqualTo(large.Length));

        Assert.That(await consumer.MoveNextAsync(), Is.True);
        Assert.That(await BodyText(consumer), Is.EqualTo("after"),
            "the oversized record desynchronised the stream");
    }

    /// <summary>A caught up consumer parks, and the publish that follows is what releases it.</summary>
    [Test]
    public async Task AParkedConsumerWakesOnAPublish()
    {
        await using var cluster = new Cluster();
        await cluster.CreateTopic("events", 1);

        await using var consumer = cluster.Consumer(["events"],
            new ConsumerOptions { MaxWaitMilliseconds = 30_000 });
        await consumer.ConnectAsync();

        var moving = consumer.MoveNextAsync().AsTask();
        await cluster.Publish("events", 0, "k", "late");

        Assert.That(await moving.WaitAsync(TimeSpan.FromSeconds(30)), Is.True);
        Assert.That(await BodyText(consumer), Is.EqualTo("late"));
    }

    /// <summary>A record body is only valid until the next move, so skipping one must still resync.</summary>
    [Test]
    public async Task SkippingABodyStillLandsOnTheNextRecord()
    {
        await using var cluster = new Cluster();
        await cluster.CreateTopic("events", 1);
        await cluster.Publish("events", 0, "a", new string('y', 70_000));
        await cluster.Publish("events", 0, "b", "second");

        await using var consumer = cluster.Consumer(["events"]);

        Assert.That(await consumer.MoveNextAsync(), Is.True);
        Assert.That(await consumer.MoveNextAsync(), Is.True);
        Assert.That(Encoding.UTF8.GetString(consumer.Key.Span), Is.EqualTo("b"));
        Assert.That(await BodyText(consumer), Is.EqualTo("second"));
    }

    [Test]
    public async Task AnUnknownTopicIsReportedOnConnect()
    {
        await using var cluster = new Cluster();
        await using var consumer = cluster.Consumer(["missing"]);

        Assert.That(async () => await consumer.ConnectAsync(), Throws.InstanceOf<InvalidOperationException>());
    }

    [Test]
    public async Task ReadingBeforeMovingThrows()
    {
        await using var cluster = new Cluster();
        await cluster.CreateTopic("events", 1);
        await using var consumer = cluster.Consumer(["events"]);

        Assert.That(() => consumer.Current, Throws.InstanceOf<InvalidOperationException>());
    }

    private static async Task<List<(string Key, string Body)>> Drain(ToyStreamConsumer consumer)
    {
        var records = new List<(string, string)>();
        while (await consumer.MoveNextAsync())
        {
            records.Add((Encoding.UTF8.GetString(consumer.Key.Span), await BodyText(consumer)));
        }

        return records;
    }

    private static async Task<string> BodyText(ToyStreamConsumer consumer)
    {
        var sink = new BufferWriter();
        await consumer.CopyBodyTo(sink);
        return Encoding.UTF8.GetString(sink.WrittenSpan);
    }

    /// <summary>A server with its own topics, plus the clients that talk to it over a pipe pair.</summary>
    private sealed class Cluster : IAsyncDisposable
    {
        private readonly CancellationTokenSource cancellation = new();
        private readonly InMemoryTopicService service = new();
        private readonly List<IAsyncDisposable> clients = [];
        private readonly Transport transport;

        public Cluster() => this.transport = new Transport(this.service, this.cancellation.Token);

        public async Task CreateTopic(string name, short partitions)
        {
            var admin = new ToyStreamAdminClient(this.transport);
            this.clients.Add(admin);

            Assert.That(await admin.CreateTopic(name, partitions), Is.EqualTo(CreateTopicResult.Success));
        }

        public async Task Publish(string topic, short partition, string key, string body)
        {
            var producer = new ToyStreamProducer(this.transport);
            this.clients.Add(producer);

            var payload = Encoding.UTF8.GetBytes(body);
            var pipe = new Pipe();
            var writing = Feed(pipe.Writer, payload);

            Assert.That(
                await producer.Publish(topic, partition, Encoding.UTF8.GetBytes(key), pipe.Reader, payload.Length),
                Is.EqualTo(PublishResult.Success));

            await writing;

            // A payload past the pipe's threshold has to be fed as the reader drains it.
            static async Task Feed(PipeWriter writer, byte[] payload)
            {
                await writer.WriteAsync(payload);
                await writer.CompleteAsync();
            }
        }

        public ToyStreamConsumer Consumer(string[] topics, ConsumerOptions? options = null)
        {
            var consumer = new ToyStreamConsumer(this.transport, topics,
                options ?? new ConsumerOptions { MaxWaitMilliseconds = 0 });
            this.clients.Add(consumer);
            return consumer;
        }

        public async ValueTask DisposeAsync()
        {
            foreach (var client in this.clients)
            {
                await client.DisposeAsync();
            }

            await this.cancellation.CancelAsync();
            this.cancellation.Dispose();
        }

        private sealed class Transport(ITopicService service, CancellationToken cancellationToken) : IClientTransport
        {
            public ValueTask<ITransportConnection> ConnectAsync(CancellationToken token)
            {
                var clientToServer = new Pipe();
                var serverToClient = new Pipe();

                // The real accept path, so the consumer's handshake is exercised rather than skipped.
                _ = TestServer.Serving(service)
                    .Accept(new PipePair(clientToServer.Reader, serverToClient.Writer), cancellationToken);

                return new ValueTask<ITransportConnection>(
                    new PipePair(serverToClient.Reader, clientToServer.Writer));
            }
        }
    }

    private sealed class BufferWriter : PipeWriter
    {
        private readonly ArrayBufferWriter<byte> buffer = new(initialCapacity: 1024);

        public ReadOnlySpan<byte> WrittenSpan => this.buffer.WrittenSpan;

        public override void Advance(int bytes) => this.buffer.Advance(bytes);

        public override Memory<byte> GetMemory(int sizeHint = 0) => this.buffer.GetMemory(sizeHint);

        public override Span<byte> GetSpan(int sizeHint = 0) => this.buffer.GetSpan(sizeHint);

        public override void CancelPendingFlush()
        {
        }

        public override void Complete(Exception? exception = null)
        {
        }

        public override ValueTask<FlushResult> FlushAsync(CancellationToken cancellationToken = default) =>
            new(new FlushResult(isCanceled: false, isCompleted: false));
    }
}
