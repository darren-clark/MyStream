namespace ToyStream.Server.Test;

using System.Buffers;
using System.IO.Pipelines;
using System.Text;
using ToyStream.Protocol;
using ToyStream.Protocol.Messages;
using ToyStream.Protocol.Responses;
using ToyStream.Server.InMemory;
using ToyStream.Shared;

/// <summary>
/// Fetch answers with a streamed body, so these drive a real connection pair rather than decoding
/// the handler's bytes by hand: the framing is only correct if a reader can walk it.
/// </summary>
public class FetchTests
{
    private static readonly Utf8String[] OneTopic = [new Utf8String("events")];

    private readonly record struct Record(FetchedRecordHeader Header, byte[] Key, byte[] Body);

    [Test]
    public async Task FetchReturnsWhatWasPublished()
    {
        var service = ServiceWithTopic("events", 2);
        await Publish(service, "events", 0, "k0", "first");
        await Publish(service, "events", 0, "k1", "second");

        var records = await Fetch(service, OneTopic, [new FetchEntry(0, 0, 0)]);

        Assert.That(records, Has.Count.EqualTo(2));
        Assert.That(records[0].Header.Offset, Is.EqualTo(0));
        Assert.That(Text(records[0].Key), Is.EqualTo("k0"));
        Assert.That(Text(records[0].Body), Is.EqualTo("first"));
        Assert.That(records[1].Header.Offset, Is.EqualTo(1));
        Assert.That(Text(records[1].Body), Is.EqualTo("second"));
    }

    [Test]
    public async Task FetchResumesFromTheGivenOffset()
    {
        var service = ServiceWithTopic("events", 1);
        await Publish(service, "events", 0, "a", "one");
        await Publish(service, "events", 0, "b", "two");
        await Publish(service, "events", 0, "c", "three");

        var records = await Fetch(service, OneTopic, [new FetchEntry(0, 0, 2)]);

        Assert.That(records, Has.Count.EqualTo(1));
        Assert.That(Text(records[0].Body), Is.EqualTo("three"));
    }

    /// <summary>A caught up consumer gets an empty success, not a failure.</summary>
    [Test]
    public async Task FetchAtTheEndReturnsNothing()
    {
        var service = ServiceWithTopic("events", 1);
        await Publish(service, "events", 0, "a", "one");

        Assert.That(await Fetch(service, OneTopic, [new FetchEntry(0, 0, 1)]), Is.Empty);
    }

    /// <summary>
    /// The point of streaming the response: a record larger than a chunk, and larger than the pipe
    /// will hold, still comes back whole.
    /// </summary>
    [Test]
    public async Task RecordsLargerThanAChunkRoundTrip()
    {
        var service = ServiceWithTopic("events", 1);
        var large = new string('x', (ProtocolConstants.MaxChunkSize * 3) / 2);
        await Publish(service, "events", 0, "big", large);
        await Publish(service, "events", 0, "small", "after");

        var records = await Fetch(service, OneTopic, [new FetchEntry(0, 0, 0)],
            maxBytes: ProtocolConstants.MaxChunkSize * 4);

        Assert.That(records, Has.Count.EqualTo(2));
        Assert.That(records[0].Body, Has.Length.EqualTo(large.Length));
        Assert.That(Text(records[1].Body), Is.EqualTo("after"), "the oversized record desynchronised the stream");
    }

    /// <summary>One request covers every partition, which is the whole reason it takes a list.</summary>
    [Test]
    public async Task FetchSpansPartitionsAndTopics()
    {
        var service = ServiceWithTopic("events", 2);
        Assert.That(CreateTopic(service, "audit", 1), Is.EqualTo(CreateTopicResult.Success));

        await Publish(service, "events", 0, "a", "events-0");
        await Publish(service, "events", 1, "b", "events-1");
        await Publish(service, "audit", 0, "c", "audit-0");

        var records = await Fetch(service, [new Utf8String("events"), new Utf8String("audit")],
            [new FetchEntry(0, 0, 0), new FetchEntry(0, 1, 0), new FetchEntry(1, 0, 0)]);

        Assert.That(records.Select(r => Text(r.Body)),
            Is.EquivalentTo(new[] { "events-0", "events-1", "audit-0" }));
        Assert.That(records.Select(r => (r.Header.TopicIndex, r.Header.PartitionId)),
            Is.EquivalentTo(new[] { ((short)0, (short)0), ((short)0, (short)1), ((short)1, (short)0) }));
    }

    [Test]
    public async Task MaxBytesStopsTheResponseGrowing()
    {
        var service = ServiceWithTopic("events", 1);
        for (var i = 0; i < 10; i++)
        {
            await Publish(service, "events", 0, "k", new string('x', 1000));
        }

        var records = await Fetch(service, OneTopic, [new FetchEntry(0, 0, 0)], maxBytes: 2500);

        Assert.That(records, Has.Count.LessThan(10), "maxBytes was ignored");
        Assert.That(records, Is.Not.Empty, "maxBytes starved the response");
        Assert.That(records[0].Header.Offset, Is.EqualTo(0));
    }

    /// <summary>The index comes off the wire, so it must be checked before it reaches the table.</summary>
    [TestCase((short)-1)]
    [TestCase((short)1)]
    [TestCase(short.MaxValue)]
    public async Task OutOfRangeTopicIndexIsRejected(short topicIndex)
    {
        var service = ServiceWithTopic("events", 1);

        Assert.That(await FetchResultOf(service, OneTopic, [new FetchEntry(topicIndex, 0, 0)]),
            Is.EqualTo(FetchResult.UnknownTopic));
    }

    [TestCase((short)-1)]
    [TestCase((short)4)]
    public async Task OutOfRangePartitionIsRejected(short partitionId)
    {
        var service = ServiceWithTopic("events", 2);

        Assert.That(await FetchResultOf(service, OneTopic, [new FetchEntry(0, partitionId, 0)]),
            Is.EqualTo(FetchResult.UnknownPartition));
    }

    [TestCase(-1L)]
    [TestCase(5L)]
    public async Task OutOfRangeOffsetIsRejected(long offset)
    {
        var service = ServiceWithTopic("events", 1);
        await Publish(service, "events", 0, "a", "one");

        Assert.That(await FetchResultOf(service, OneTopic, [new FetchEntry(0, 0, offset)]),
            Is.EqualTo(FetchResult.OffsetOutOfRange));
    }

    [Test]
    public async Task UnknownTopicNameIsRejected()
    {
        var service = ServiceWithTopic("events", 1);

        Assert.That(await FetchResultOf(service, [new Utf8String("missing")], [new FetchEntry(0, 0, 0)]),
            Is.EqualTo(FetchResult.UnknownTopic));
    }

    /// <summary>
    /// A failure still carries an empty body, so the connection is usable afterwards rather than
    /// stalled waiting for a stream that was never written.
    /// </summary>
    [Test]
    public async Task AConnectionSurvivesAFailedFetch()
    {
        var service = ServiceWithTopic("events", 1);
        await Publish(service, "events", 0, "a", "one");
        await using var connected = Connect(service);

        Assert.That(await Drain(connected, [new Utf8String("missing")], [new FetchEntry(0, 0, 0)]),
            Is.Empty);

        var records = await Drain(connected, OneTopic, [new FetchEntry(0, 0, 0)]);
        Assert.That(Text(records.Single().Body), Is.EqualTo("one"));
    }

    /// <summary>A caught up fetch parks: answering it immediately would return nothing.</summary>
    [Test]
    public async Task FetchWaitsForAPublish()
    {
        var service = ServiceWithTopic("events", 1);
        await using var connected = Connect(service);

        var fetch = SendFetch(connected, OneTopic, [new FetchEntry(0, 0, 0)], maxWaitMilliseconds: 30_000);
        await Publish(service, "events", 0, "k", "late");

        var records = await DrainResponse(await fetch.WaitAsync(TimeSpan.FromSeconds(30)));

        Assert.That(records, Has.Count.EqualTo(1));
        Assert.That(Text(records[0].Body), Is.EqualTo("late"));
    }

    /// <summary>A park that times out answers empty rather than failing.</summary>
    [Test]
    public async Task FetchGivesUpAfterTheWait()
    {
        var service = ServiceWithTopic("events", 1);
        await using var connected = Connect(service);

        var response = await SendFetch(connected, OneTopic, [new FetchEntry(0, 0, 0)], maxWaitMilliseconds: 50)
            .WaitAsync(TimeSpan.FromSeconds(30));

        Assert.That(response.Result, Is.EqualTo(FetchResult.Success));
        Assert.That(await DrainResponse(response), Is.Empty);
    }

    /// <summary>A publish to any requested partition releases the park, not just the first.</summary>
    [Test]
    public async Task FetchWakesOnAnyRequestedPartition()
    {
        var service = ServiceWithTopic("events", 3);
        await using var connected = Connect(service);

        var fetch = SendFetch(connected, OneTopic,
            [new FetchEntry(0, 0, 0), new FetchEntry(0, 1, 0), new FetchEntry(0, 2, 0)],
            maxWaitMilliseconds: 30_000);

        await Publish(service, "events", 2, "k", "third-partition");

        var records = await DrainResponse(await fetch.WaitAsync(TimeSpan.FromSeconds(30)));

        Assert.That(records, Has.Count.EqualTo(1));
        Assert.That(records[0].Header.PartitionId, Is.EqualTo(2));
    }

    /// <summary>
    /// A publish can land between the park deciding there is nothing to send and the park attaching
    /// to the signal, which is the one window in which a wake can go missing. Repeated because the
    /// window is narrow and only ever hit by chance.
    /// </summary>
    [Test]
    public async Task APublishRacingTheParkStillWakesIt()
    {
        for (var attempt = 0; attempt < 100; attempt++)
        {
            var service = ServiceWithTopic("events", 1);
            await using var connected = Connect(service);

            var fetch = SendFetch(connected, OneTopic, [new FetchEntry(0, 0, 0)], maxWaitMilliseconds: 30_000);
            var publishing = Publish(service, "events", 0, "k", "raced");

            var records = await DrainResponse(await fetch.WaitAsync(TimeSpan.FromSeconds(10)));
            await publishing;

            Assert.That(records, Is.Not.Empty, $"attempt {attempt} lost the wake");
        }
    }

    /// <summary>The connection closing must not wait out the full park.</summary>
    [Test]
    public async Task CancellingTheConnectionReleasesAParkedFetch()
    {
        var service = ServiceWithTopic("events", 1);
        using var cancellation = new CancellationTokenSource();

        IRequestHandler<FetchRequestMessage, FetchResponseMessage> handler = new ServerHandler(service);
        var fetch = handler.DispatchRequest(
            new FetchRequestMessage
            {
                MaxWaitMilliseconds = 30_000,
                MaxBytes = 65536,
                Topics = new TopicNames(OneTopic),
                Entries = new FetchEntries([new FetchEntry(0, 0, 0)])
            },
            new ResponseWriter<FetchResponseMessage>(new TestPipeWriter()), cancellation.Token);

        Assert.That(fetch.IsCompleted, Is.False);

        await cancellation.CancelAsync();

        Assert.That(async () => await fetch.AsTask().WaitAsync(TimeSpan.FromSeconds(30)),
            Throws.InstanceOf<OperationCanceledException>());
    }

    private static Connected Connect(ITopicService service)
    {
        var clientToServer = new Pipe();
        var serverToClient = new Pipe();
        var cancellation = new CancellationTokenSource();

        var server = new Connection<ServerHandler>(
            clientToServer.Reader, serverToClient.Writer, new ServerHandler(service));
        var client = new Connection<NoRequestHandler>(
            serverToClient.Reader, clientToServer.Writer, new NoRequestHandler());

        _ = server.Start(cancellation.Token);
        _ = client.Start(cancellation.Token);

        return new Connected(client, cancellation);
    }

    private static Task<FetchResponse> SendFetch(Connected connected, Utf8String[] topics, FetchEntry[] entries,
        int maxWaitMilliseconds, int maxBytes = 65536) =>
        connected.Client
            .SendMessageForStreamedResponse<FetchRequestMessage, FetchResponseMessage, FetchResponse>(
                new FetchRequestMessage
                {
                    MaxWaitMilliseconds = maxWaitMilliseconds,
                    MaxBytes = maxBytes,
                    Topics = new TopicNames(topics),
                    Entries = new FetchEntries(entries)
                }, connected.Cancellation.Token)
            .AsTask();

    private static async Task<List<Record>> Fetch(ITopicService service, Utf8String[] topics, FetchEntry[] entries,
        int maxBytes = 65536)
    {
        await using var connected = Connect(service);
        return await Drain(connected, topics, entries, maxBytes);
    }

    private static async Task<FetchResult> FetchResultOf(ITopicService service, Utf8String[] topics,
        FetchEntry[] entries)
    {
        await using var connected = Connect(service);
        var response = await SendFetch(connected, topics, entries, maxWaitMilliseconds: 0)
            .WaitAsync(TimeSpan.FromSeconds(30));

        await using (response)
        {
            return response.Result;
        }
    }

    private static async Task<List<Record>> Drain(Connected connected, Utf8String[] topics, FetchEntry[] entries,
        int maxBytes = 65536) =>
        await DrainResponse(await SendFetch(connected, topics, entries, maxWaitMilliseconds: 0, maxBytes)
            .WaitAsync(TimeSpan.FromSeconds(30)));

    private static async Task<List<Record>> DrainResponse(FetchResponse response)
    {
        await using (response)
        {
            var records = new List<Record>();
            while (await response.MoveNextAsync())
            {
                var body = new TestPipeWriter();
                await response.CopyBodyTo(body);
                records.Add(new Record(response.Current, response.Key.ToArray(), body.WrittenSpan.ToArray()));
            }

            return records;
        }
    }

    private static string Text(byte[] value) => Encoding.UTF8.GetString(value);

    private static InMemoryTopicService ServiceWithTopic(string name, short partitions)
    {
        var service = new InMemoryTopicService();
        Assert.That(CreateTopic(service, name, partitions), Is.EqualTo(CreateTopicResult.Success));
        return service;
    }

    private static CreateTopicResult CreateTopic(ITopicService service, string topicName, short partitionCount)
    {
        var buffer = new TestPipeWriter();
        IRequestHandler<CreateTopicMessage, AcknowledgeMessage<CreateTopicResult>> handler =
            new ServerHandler(service);

        var dispatched = handler.DispatchRequest(
            new CreateTopicMessage { TopicName = new Utf8String(topicName), PartitionCount = partitionCount },
            new ResponseWriter<AcknowledgeMessage<CreateTopicResult>>(buffer), default);

        Assert.That(dispatched.IsCompletedSuccessfully, Is.True);
        return (CreateTopicResult)System.Buffers.Binary.BinaryPrimitives.ReadInt32BigEndian(
            buffer.WrittenSpan[1..]);
    }

    private static async Task Publish(ITopicService service, string topic, short partition, string key, string body)
    {
        var bodyPipe = new Pipe();
        var payload = Encoding.UTF8.GetBytes(body);
        var writing = WritePayload(bodyPipe.Writer, payload);

        var buffer = new TestPipeWriter();
        IStreamingRequestHandler<PublishMessage, AcknowledgeMessage<PublishResult>> handler =
            new ServerHandler(service);

        await handler.DispatchRequest(
            new StreamingMessage<PublishMessage>
            {
                Header = new PublishMessage
                {
                    Topic = new Utf8String(topic),
                    Partition = partition,
                    Key = new RefBytes(Encoding.UTF8.GetBytes(key))
                },
                Size = payload.Length,
                Body = bodyPipe.Reader
            },
            new ResponseWriter<AcknowledgeMessage<PublishResult>>(buffer), default);

        await writing;

        // A payload past the pipe's threshold has to be fed as the reader drains it.
        static async Task WritePayload(PipeWriter writer, byte[] payload)
        {
            await writer.WriteAsync(payload);
            await writer.CompleteAsync();
        }
    }

    private sealed class Connected(ConnectionBase client, CancellationTokenSource cancellation) : IAsyncDisposable
    {
        public ConnectionBase Client => client;

        public CancellationTokenSource Cancellation => cancellation;

        public async ValueTask DisposeAsync()
        {
            await cancellation.CancelAsync();
            cancellation.Dispose();
        }
    }

    private readonly struct NoRequestHandler : IConnectionHandler
    {
        public DispatchResult TryParseAndDispatch(MessageType messageType, ref Dispatcher dispatch) =>
            dispatch.Unexpected(messageType);
    }

    private sealed class TestPipeWriter : PipeWriter
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
