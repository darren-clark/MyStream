using ToyStream.Server;

namespace ToyStream.Protocol.Test;

using System.Buffers;
using System.IO.Pipelines;
using System.Net.Sockets;
using ToyStream.Client;
using ToyStream.Protocol.Messages;
using ToyStream.Protocol.Responses;

/// <summary>
/// The role clients compose a transport rather than a live pipe pair, so these exercise them over
/// an in-process one with no socket involved.
/// </summary>
public class ClientTests
{
    [Test]
    public async Task ConstructionDoesNotConnect()
    {
        await using var transport = new InMemoryTransport();
        await using var admin = new ToyStreamAdminClient(transport);
        await using var producer = new ToyStreamProducer(transport);

        Assert.That(transport.ConnectCount, Is.Zero);
    }

    [Test]
    public async Task ConnectsOnFirstUseAndReusesTheConnection()
    {
        await using var transport = new InMemoryTransport();
        await using var admin = new ToyStreamAdminClient(transport);

        Assert.That(await admin.CreateTopic("first", 4), Is.EqualTo(CreateTopicResult.Success));
        Assert.That(transport.ConnectCount, Is.EqualTo(1));

        Assert.That(await admin.CreateTopic("second", 2), Is.EqualTo(CreateTopicResult.Success));
        Assert.That(transport.ConnectCount, Is.EqualTo(1), "the second call reconnected");
    }

    [Test]
    public async Task ConnectAsyncConnectsAndIsIdempotent()
    {
        await using var transport = new InMemoryTransport();
        await using var admin = new ToyStreamAdminClient(transport);

        await admin.ConnectAsync();
        Assert.That(transport.ConnectCount, Is.EqualTo(1));

        await admin.ConnectAsync();
        Assert.That(transport.ConnectCount, Is.EqualTo(1), "a second connect opened another transport");

        Assert.That(await admin.CreateTopic("topic", 1), Is.EqualTo(CreateTopicResult.Success));
        Assert.That(transport.ConnectCount, Is.EqualTo(1));
    }

    /// <summary>The point of connecting up front is that a bad endpoint surfaces here, not mid-request.</summary>
    [Test]
    public async Task ConnectAsyncSurfacesTransportFailure()
    {
        await using var admin = new ToyStreamAdminClient(new FailingTransport());

        Assert.That(async () => await admin.ConnectAsync(), Throws.InstanceOf<SocketException>());
    }

    [Test]
    public async Task ConnectAsyncCanBeRetriedAfterFailure()
    {
        var transport = new FailingTransport();
        await using var admin = new ToyStreamAdminClient(transport);

        Assert.That(async () => await admin.ConnectAsync(), Throws.InstanceOf<SocketException>());

        transport.Fail = false;
        Assert.That(async () => await admin.ConnectAsync(), Throws.Nothing);
    }

    [Test]
    public async Task ConnectAsyncAfterDisposeThrows()
    {
        await using var transport = new InMemoryTransport();
        var admin = new ToyStreamAdminClient(transport);
        await admin.DisposeAsync();

        Assert.That(async () => await admin.ConnectAsync(), Throws.InstanceOf<ObjectDisposedException>());
    }

    [Test]
    public async Task UseAfterDisposeThrows()
    {
        await using var transport = new InMemoryTransport();
        var admin = new ToyStreamAdminClient(transport);

        await admin.CreateTopic("topic", 1);
        await admin.DisposeAsync();

        Assert.That(async () => await admin.CreateTopic("topic", 1),
            Throws.InstanceOf<ObjectDisposedException>());
    }

    [Test]
    public async Task DisposeWithoutConnectingIsSafe()
    {
        await using var transport = new InMemoryTransport();
        var producer = new ToyStreamProducer(transport);

        Assert.That(async () => await producer.DisposeAsync(), Throws.Nothing);
    }

    [TestCase(5)]
    [TestCase(128 * 1024)]
    public async Task ProducerPublishesBody(int bodySize)
    {
        await using var transport = new InMemoryTransport();
        await using var producer = new ToyStreamProducer(transport);

        var body = RandomBytes(bodySize);
        Assert.That(await producer.Publish("events", 0, BodyReader(body), body.Length),
            Is.EqualTo(PublishResult.Success));
        Assert.That(transport.Published.Bytes, Is.EqualTo(body));
    }

    /// <summary>Size is advisory, so an unknown-length body must round trip too.</summary>
    [Test]
    public async Task ProducerPublishesBodyOfUnknownSize()
    {
        await using var transport = new InMemoryTransport();
        await using var producer = new ToyStreamProducer(transport);

        var body = RandomBytes(2048);
        Assert.That(await producer.Publish("events", 0, BodyReader(body)), Is.EqualTo(PublishResult.Success));
        Assert.That(transport.Published.Bytes, Is.EqualTo(body));
    }

    /// <summary>The key rides in the header, so it must arrive intact alongside a streamed body.</summary>
    [TestCase(1)]
    [TestCase(ProtocolConstants.MaxKeySize)]
    public async Task ProducerPublishesKeyWithBody(int keySize)
    {
        await using var transport = new InMemoryTransport();
        await using var producer = new ToyStreamProducer(transport);

        var key = RandomBytes(keySize);
        var body = RandomBytes(4096);

        Assert.That(await producer.Publish("events", 0, key, BodyReader(body), body.Length),
            Is.EqualTo(PublishResult.Success));
        Assert.That(transport.Published.Key, Is.EqualTo(key));
        Assert.That(transport.Published.Bytes, Is.EqualTo(body));
    }

    [Test]
    public async Task ProducerPublishesWithoutAKey()
    {
        await using var transport = new InMemoryTransport();
        await using var producer = new ToyStreamProducer(transport);

        var body = RandomBytes(64);
        Assert.That(await producer.Publish("events", 0, BodyReader(body), body.Length),
            Is.EqualTo(PublishResult.Success));
        Assert.That(transport.Published.Key, Is.Empty);
    }

    /// <summary>Bounded so a key can never become a second unbounded stream.</summary>
    [Test]
    public async Task ProducerRejectsAnOversizedKey()
    {
        await using var transport = new InMemoryTransport();
        await using var producer = new ToyStreamProducer(transport);

        var key = RandomBytes(ProtocolConstants.MaxKeySize + 1);
        Assert.That(async () => await producer.Publish("events", 0, key, BodyReader(RandomBytes(16)), 16),
            Throws.InstanceOf<InvalidOperationException>());
    }

    /// <summary>
    /// The roles are separate precisely so a streamed publish cannot hold the connection against an
    /// admin request, which the single-flight guard would otherwise reject.
    /// </summary>
    [Test]
    public async Task AdminAndProducerDoNotShareAConnection()
    {
        await using var transport = new InMemoryTransport();
        await using var admin = new ToyStreamAdminClient(transport);
        await using var producer = new ToyStreamProducer(transport);

        await admin.ConnectAsync();
        await producer.ConnectAsync();

        Assert.That(transport.ConnectCount, Is.EqualTo(2));

        Assert.That(await admin.CreateTopic("events", 1), Is.EqualTo(CreateTopicResult.Success));
        Assert.That(await producer.Publish("events", 0, BodyReader(RandomBytes(64)), 64),
            Is.EqualTo(PublishResult.Success));
    }

    private static byte[] RandomBytes(int count)
    {
        var bytes = new byte[count];
        Random.Shared.NextBytes(bytes);
        return bytes;
    }

    private static PipeReader BodyReader(byte[] body)
    {
        var pipe = new Pipe();
        pipe.Writer.Write(body);
        pipe.Writer.Complete();
        return pipe.Reader;
    }

    /// <summary>Stands in for an unreachable endpoint.</summary>
    private sealed class FailingTransport : IClientTransport
    {
        public bool Fail { get; set; } = true;

        public ValueTask<ITransportConnection> ConnectAsync(CancellationToken cancellationToken)
        {
            if (Fail)
            {
                throw new SocketException((int)SocketError.ConnectionRefused);
            }

            return new InMemoryTransport().ConnectAsync(cancellationToken);
        }
    }

    /// <summary>Runs a real server read loop over a pipe pair, so the clients see genuine responses.</summary>
    private sealed class InMemoryTransport : IClientTransport, IAsyncDisposable
    {
        private readonly CancellationTokenSource cancellation = new();

        public int ConnectCount { get; private set; }

        public BodySink Published { get; } = new();

        public ValueTask<ITransportConnection> ConnectAsync(CancellationToken cancellationToken)
        {
            ConnectCount++;

            var clientToServer = new Pipe();
            var serverToClient = new Pipe();

            var server = new Connection<EchoingServerHandler>(
                clientToServer.Reader, serverToClient.Writer, new EchoingServerHandler(Published));
            _ = server.Start(this.cancellation.Token);

            return new ValueTask<ITransportConnection>(
                new PipePair(serverToClient.Reader, clientToServer.Writer));
        }

        public async ValueTask DisposeAsync()
        {
            await this.cancellation.CancelAsync();
            this.cancellation.Dispose();
        }

        private sealed class PipePair(PipeReader input, PipeWriter output) : ITransportConnection
        {
            public PipeReader Input => input;

            public PipeWriter Output => output;

            public ValueTask DisposeAsync() => default;
        }
    }

    internal sealed class BodySink
    {
        public byte[] Bytes { get; private set; } = [];

        public byte[] Key { get; set; } = [];

        /// <summary>Deliberately the canonical pipe loop, so the body reader must tolerate it.</summary>
        public async ValueTask ReadAll(PipeReader body, CancellationToken cancellationToken)
        {
            var buffer = new ArrayBufferWriter<byte>();
            while (true)
            {
                var result = await body.ReadAsync(cancellationToken);
                foreach (var segment in result.Buffer)
                {
                    buffer.Write(segment.Span);
                }

                body.AdvanceTo(result.Buffer.End);
                if (result.IsCompleted)
                {
                    break;
                }
            }

            Bytes = buffer.WrittenSpan.ToArray();
        }
    }

    private readonly struct EchoingServerHandler(BodySink published)
        : IConnectionHandler,
            IRequestHandler<CreateTopicMessage, AcknowledgeMessage<CreateTopicResult>>,
            IStreamingRequestHandler<PublishMessage, AcknowledgeMessage<PublishResult>>
    {
        public DispatchResult TryParseAndDispatch(MessageType messageType, ref Dispatcher dispatch) =>
            messageType switch
            {
                MessageType.CreateTopic =>
                    dispatch.Request<EchoingServerHandler, CreateTopicMessage,
                        AcknowledgeMessage<CreateTopicResult>>(this),
                MessageType.PublishEvent =>
                    dispatch.StreamingRequest<EchoingServerHandler, PublishMessage,
                        AcknowledgeMessage<PublishResult>>(this),
                _ => dispatch.Unexpected(messageType)
            };

        public ValueTask DispatchRequest(CreateTopicMessage message,
            ResponseWriter<AcknowledgeMessage<CreateTopicResult>> responseWriter, CancellationToken cancellationToken)
        {
            responseWriter.WriteResponse(AcknowledgeMessage.WithResult(CreateTopicResult.Success));
            return default;
        }

        public ValueTask DispatchRequest(StreamingMessage<PublishMessage> message,
            ResponseWriter<AcknowledgeMessage<PublishResult>> responseWriter, CancellationToken cancellationToken)
        {
            // Resolved now: the key is a view over a buffer the first await releases.
            published.Key = message.Header.Key.ToArray();
            return ReadAndAcknowledge(published, message.Body, responseWriter, cancellationToken);
        }

        private static async ValueTask ReadAndAcknowledge(BodySink sink, PipeReader body,
            ResponseWriter<AcknowledgeMessage<PublishResult>> responseWriter, CancellationToken cancellationToken)
        {
            await sink.ReadAll(body, cancellationToken);
            responseWriter.WriteResponse(AcknowledgeMessage.WithResult(PublishResult.Success));
        }
    }
}
