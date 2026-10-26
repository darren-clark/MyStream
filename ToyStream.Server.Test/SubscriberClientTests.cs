namespace ToyStream.Server.Test;

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

/// <summary>Runs the real accept over a pipe pair, so a client sees a genuine server.</summary>
internal sealed class SubscriptionHarness(ITopicService service) : IAsyncDisposable
{
    private readonly CancellationTokenSource cancellation = new();
    private readonly List<ToyStreamSubscriber> subscribers = [];

    public ToyStreamSubscriber Subscriber(string topic, SubscriberOptions? options = null)
    {
        var subscriber = new ToyStreamSubscriber(new Transport(service, this.cancellation.Token), topic, options);
        this.subscribers.Add(subscriber);
        return subscriber;
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

    private sealed class Transport(ITopicService service, CancellationToken cancellationToken) : IClientTransport
    {
        public ValueTask<ITransportConnection> ConnectAsync(CancellationToken token)
        {
            var clientToServer = new Pipe();
            var serverToClient = new Pipe();

            _ = TestServer.Serving(service)
                .Accept(new PipePair(clientToServer.Reader, serverToClient.Writer), cancellationToken);

            return new ValueTask<ITransportConnection>(
                new PipePair(serverToClient.Reader, clientToServer.Writer));
        }
    }
}
