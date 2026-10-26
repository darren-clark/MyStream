namespace ToyStream.Server.Test;

using System.Buffers;
using System.IO.Pipelines;
using System.Text;
using ToyStream.Protocol;
using ToyStream.Protocol.Messages;
using ToyStream.Protocol.Responses;
using ToyStream.Server.InMemory;
using ToyStream.Shared;

public class SubscriptionTests
{
    [Test]
    public async Task RecordsAlreadyInTheLogArePushedOnSubscribe()
    {
        var service = ServiceWithTopic("events", 1);
        await Publish(service, "events", 0, "k1", "first");
        await Publish(service, "events", 0, "k2", "second");

        await using var subscriber = Subscribe(service, "events", SubscribeOffset.Earliest);

        Assert.That(await subscriber.Sink.Take(2), Is.EqualTo(new[] { "first", "second" }));
    }

    [Test]
    public async Task RecordsPublishedAfterSubscribeArePushed()
    {
        var service = ServiceWithTopic("events", 1);

        await using var subscriber = Subscribe(service, "events", SubscribeOffset.Earliest);

        await Publish(service, "events", 0, "k1", "later");

        Assert.That(await subscriber.Sink.Take(1), Is.EqualTo(new[] { "later" }));
    }

    /// <summary>Latest means the backlog is skipped, which is the only thing the offset decides.</summary>
    [Test]
    public async Task SubscribingAtLatestSkipsTheBacklog()
    {
        var service = ServiceWithTopic("events", 1);
        await Publish(service, "events", 0, "k1", "before");

        await using var subscriber = Subscribe(service, "events", SubscribeOffset.Latest);
        await subscriber.Sink.Established.WaitAsync(TimeSpan.FromSeconds(10));

        await Publish(service, "events", 0, "k2", "after");

        Assert.That(await subscriber.Sink.Take(1), Is.EqualTo(new[] { "after" }));
    }

    /// <summary>
    /// A subscriber connection has no reader for a response, so the only way to report a bad
    /// subscription is a message going the same way as the records.
    /// </summary>
    [Test]
    public async Task SubscribingToAnUnknownTopicLosesTheSubscription()
    {
        var service = new InMemoryTopicService();

        await using var subscriber = Subscribe(service, "missing", SubscribeOffset.Earliest);

        Assert.That(await subscriber.Sink.Lost.WaitAsync(TimeSpan.FromSeconds(10)),
            Is.EqualTo(SubscriptionLostReason.UnknownTopic));
    }

    /// <summary>
    /// Everything available goes out together, so a backlog costs one message rather than one per
    /// record. This is the whole batching story - there is no size to configure.
    /// </summary>
    [Test]
    public async Task ABacklogIsPushedAsOneBatch()
    {
        var service = ServiceWithTopic("events", 1);
        for (var i = 0; i < 5; i++)
        {
            await Publish(service, "events", 0, $"k{i}", $"body-{i}");
        }

        await using var subscriber = Subscribe(service, "events", SubscribeOffset.Earliest);

        await subscriber.Sink.Take(5);
        Assert.That(subscriber.Sink.RecordBatches, Is.EqualTo(1));
    }

    private static Subscriber Subscribe(ITopicService service, string topic, SubscribeOffset startOffset)
    {
        var clientToServer = new Pipe();
        var serverToClient = new Pipe();
        var cancellation = new CancellationTokenSource();

        var accepting = TestServer.Serving(service)
            .Accept(new PipePair(clientToServer.Reader, serverToClient.Writer), cancellation.Token);

        var sink = new PushSink();
        var client = new Connection<PushHandler>(
            serverToClient.Reader, clientToServer.Writer, new PushHandler(sink));
        _ = client.Start(cancellation.Token);

        var started = Handshake(client, clientToServer.Writer, topic, startOffset, cancellation.Token);

        return new Subscriber(sink, accepting, started, cancellation);

        static async Task Handshake(ConnectionBase connection, PipeWriter output, string topic,
            SubscribeOffset startOffset, CancellationToken cancellationToken)
        {
            await ProtocolHandshake.WriteAsync(output, ConnectionRole.Subscriber, cancellationToken);
            await SendSubscribe(connection, topic, startOffset, cancellationToken);
        }
    }

    // Split out because a ref struct message cannot be built inside an async method.
    private static ValueTask SendSubscribe(ConnectionBase connection, string topic, SubscribeOffset startOffset,
        CancellationToken cancellationToken) =>
        connection.SendMessage(
            new SubscribeMessage { Topic = topic, StartOffset = startOffset }, cancellationToken);

    private sealed record Subscriber(
        PushSink Sink, Task Accepting, Task Started, CancellationTokenSource Cancellation) : IAsyncDisposable
    {
        public async ValueTask DisposeAsync()
        {
            await Cancellation.CancelAsync();
            Cancellation.Dispose();
        }
    }

    private sealed class PushSink
    {
        private readonly List<string> bodies = [];
        private readonly TaskCompletionSource<SubscriptionLostReason> lost =
            new(TaskCreationOptions.RunContinuationsAsynchronously);
        private readonly TaskCompletionSource established =
            new(TaskCreationOptions.RunContinuationsAsynchronously);

        public Task<SubscriptionLostReason> Lost => this.lost.Task;

        /// <summary>The empty batch the pump sends once it has latched its offsets.</summary>
        public Task Established => this.established.Task;

        public int RecordBatches { get; private set; }

        public void Lose(SubscriptionLostReason reason) => this.lost.TrySetResult(reason);

        public async Task<string[]> Take(int count)
        {
            var deadline = DateTime.UtcNow.AddSeconds(10);
            while (DateTime.UtcNow < deadline)
            {
                lock (this.bodies)
                {
                    if (this.bodies.Count >= count)
                    {
                        return this.bodies.Take(count).ToArray();
                    }
                }

                await Task.Delay(10);
            }

            lock (this.bodies)
            {
                throw new TimeoutException($"Only {this.bodies.Count} of {count} records arrived.");
            }
        }

        public async ValueTask ReadBatch(PipeReader body, CancellationToken cancellationToken)
        {
            var records = new FetchedRecordReader(body);
            var count = 0;
            while (await records.MoveNextAsync(cancellationToken))
            {
                count++;
                var payload = new Pipe();
                await records.CopyBodyTo(payload.Writer, cancellationToken);
                await payload.Writer.CompleteAsync();

                var read = await payload.Reader.ReadAsync(cancellationToken);
                lock (this.bodies)
                {
                    this.bodies.Add(Encoding.UTF8.GetString(read.Buffer));
                }

                payload.Reader.AdvanceTo(read.Buffer.End);
            }

            if (count > 0)
            {
                RecordBatches++;
            }

            this.established.TrySetResult();
        }
    }

    private readonly struct PushHandler(PushSink sink) :
        IConnectionHandler,
        IStreamingRequestHandler<PushRecordsMessage>,
        IRequestHandler<SubscriptionLostMessage>
    {
        public DispatchResult TryParseAndDispatch(MessageType messageType, ref Dispatcher dispatch) =>
            messageType switch
            {
                MessageType.PushRecords =>
                    dispatch.StreamingRequest<PushHandler, PushRecordsMessage>(this),
                MessageType.SubscriptionLost =>
                    dispatch.Request<PushHandler, SubscriptionLostMessage>(this),
                _ => dispatch.Unexpected(messageType)
            };

        public ValueTask DispatchRequest(StreamingMessage<PushRecordsMessage> message,
            CancellationToken cancellationToken) =>
            sink.ReadBatch(message.Body, cancellationToken);

        public ValueTask DispatchRequest(SubscriptionLostMessage message, CancellationToken cancellationToken)
        {
            sink.Lose(message.Reason);
            return default;
        }
    }

    private static ITopicService ServiceWithTopic(string name, short partitions)
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

    private static async Task Publish(ITopicService service, string topic, short partition, string key, string body)
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
}
