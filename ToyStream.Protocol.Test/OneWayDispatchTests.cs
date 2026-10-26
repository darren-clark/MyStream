namespace ToyStream.Protocol.Test;

using System.Buffers;
using System.IO.Pipelines;
using ToyStream.Protocol.Messages;

/// <summary>
/// The one-way arms exist so a role whose connection is one directional cannot write back. These
/// cover both that the handler is reached and that nothing is produced in reply.
/// </summary>
public class OneWayDispatchTests
{
    private const MessageType NotifyType = (MessageType)100;
    private const MessageType NotifyStreamType = (MessageType)101;

    private static (Pipe ClientToServer, Pipe ServerToClient) CreatePipes() => (new Pipe(), new Pipe());

    [Test]
    public async Task OneWayRequestIsDispatchedAndAnswersNothing()
    {
        var (clientToServer, serverToClient) = CreatePipes();
        using var cancellation = new CancellationTokenSource();
        var recorder = new OneWayRecorder();

        var client = new Connection<SilentHandler>(
            serverToClient.Reader, clientToServer.Writer, new SilentHandler());
        await client.SendMessage(new NotifyMessage { Value = 7 }, cancellation.Token);

        var server = new Connection<OneWayHandler>(
            clientToServer.Reader, serverToClient.Writer, new OneWayHandler(recorder));
        _ = server.Start(cancellation.Token);

        await recorder.Received.WaitAsync(TimeSpan.FromSeconds(10), cancellation.Token);

        Assert.That(recorder.Values, Is.EqualTo(new short[] { 7 }));
        Assert.That(serverToClient.Reader.TryRead(out var answer), Is.False,
            "a one-way dispatch wrote something back");
        serverToClient.Reader.AdvanceTo(answer.Buffer.Start);

        await cancellation.CancelAsync();
    }

    [Test]
    public async Task OneWayStreamingRequestDeliversItsBodyAndAnswersNothing()
    {
        var (clientToServer, serverToClient) = CreatePipes();
        using var cancellation = new CancellationTokenSource();
        var recorder = new OneWayRecorder();
        var payload = new byte[] { 1, 2, 3, 4, 5 };

        var client = new Connection<SilentHandler>(
            serverToClient.Reader, clientToServer.Writer, new SilentHandler());
        await SendStream(client, 11, payload, cancellation.Token);

        var server = new Connection<OneWayHandler>(
            clientToServer.Reader, serverToClient.Writer, new OneWayHandler(recorder));
        _ = server.Start(cancellation.Token);

        await recorder.Received.WaitAsync(TimeSpan.FromSeconds(10), cancellation.Token);

        Assert.That(recorder.Values, Is.EqualTo(new short[] { 11 }));
        Assert.That(recorder.Bodies.Single(), Is.EqualTo(payload));
        Assert.That(serverToClient.Reader.TryRead(out var answer), Is.False,
            "a one-way streamed dispatch wrote something back");
        serverToClient.Reader.AdvanceTo(answer.Buffer.Start);

        await cancellation.CancelAsync();
    }

    /// <summary>
    /// The regression this path keeps producing: a message already sitting in the buffer after a
    /// streamed body gets hidden behind a wait for bytes that never arrive. A one-way body is the
    /// worst case, because nothing is written back to prompt more traffic.
    /// </summary>
    [Test]
    public async Task MessageFollowingAOneWayStreamedBodyIsStillHandled()
    {
        var (clientToServer, serverToClient) = CreatePipes();
        using var cancellation = new CancellationTokenSource();
        var recorder = new OneWayRecorder(target: 2);

        // Both buffered before the server starts, so the second is already present when the first
        // body finishes.
        var client = new Connection<SilentHandler>(
            serverToClient.Reader, clientToServer.Writer, new SilentHandler());
        await SendStream(client, 1, [9, 9, 9], cancellation.Token);
        await client.SendMessage(new NotifyMessage { Value = 2 }, cancellation.Token);

        var server = new Connection<OneWayHandler>(
            clientToServer.Reader, serverToClient.Writer, new OneWayHandler(recorder));
        _ = server.Start(cancellation.Token);

        await recorder.Received.WaitAsync(TimeSpan.FromSeconds(10), cancellation.Token);

        Assert.That(recorder.Values, Is.EqualTo(new short[] { 1, 2 }));
        await cancellation.CancelAsync();
    }

    private static async Task SendStream(ConnectionBase connection, short value, byte[] payload,
        CancellationToken cancellationToken)
    {
        var body = new Pipe();
        await body.Writer.WriteAsync(payload, cancellationToken);
        await body.Writer.CompleteAsync();

        await connection.SendStreamingMessage(new StreamingMessage<NotifyStreamMessage>
        {
            Header = new NotifyStreamMessage { Value = value },
            Size = payload.Length,
            Body = body.Reader
        }, cancellationToken);
    }

    private readonly struct NotifyMessage : IRequestMessage<NotifyMessage>
    {
        public required short Value { get; init; }

        public static MessageType Type => NotifyType;

        static bool IMessage<NotifyMessage>.TryRead<TReader>(ref TReader reader, out NotifyMessage message,
            out int bytesToRead)
        {
            var tryReader = reader;
            if (!tryReader.TryReadInt16(out var value, out bytesToRead))
            {
                message = default;
                return false;
            }

            reader = tryReader;
            message = new NotifyMessage { Value = value };
            return true;
        }

        void IMessage.Write<TWriter>(TWriter writer) => writer.Write(this.Value);
    }

    private readonly struct NotifyStreamMessage : IStreamingRequestMessage<NotifyStreamMessage>
    {
        public required short Value { get; init; }

        public static MessageType Type => NotifyStreamType;

        static bool IMessage<NotifyStreamMessage>.TryRead<TReader>(ref TReader reader,
            out NotifyStreamMessage message, out int bytesToRead)
        {
            var tryReader = reader;
            if (!tryReader.TryReadInt16(out var value, out bytesToRead))
            {
                message = default;
                return false;
            }

            reader = tryReader;
            message = new NotifyStreamMessage { Value = value };
            return true;
        }

        void IMessage.Write<TWriter>(TWriter writer) => writer.Write(this.Value);
    }

    private sealed class OneWayRecorder(int target = 1)
    {
        private readonly TaskCompletionSource reached = new();
        private readonly List<short> values = [];
        private readonly List<byte[]> bodies = [];

        public Task Received => this.reached.Task;

        public IReadOnlyList<short> Values
        {
            get
            {
                lock (this.values)
                {
                    return this.values.ToArray();
                }
            }
        }

        public IReadOnlyList<byte[]> Bodies
        {
            get
            {
                lock (this.values)
                {
                    return this.bodies.ToArray();
                }
            }
        }

        public void Record(short value, byte[]? body = null)
        {
            lock (this.values)
            {
                this.values.Add(value);
                if (body is not null)
                {
                    this.bodies.Add(body);
                }

                if (this.values.Count >= target)
                {
                    this.reached.TrySetResult();
                }
            }
        }
    }

    private readonly struct OneWayHandler(OneWayRecorder recorder) :
        IConnectionHandler,
        IRequestHandler<NotifyMessage>,
        IStreamingRequestHandler<NotifyStreamMessage>
    {
        public DispatchResult TryParseAndDispatch(MessageType messageType, ref Dispatcher dispatch) =>
            messageType switch
            {
                NotifyType => dispatch.Request<OneWayHandler, NotifyMessage>(this),
                NotifyStreamType => dispatch.StreamingRequest<OneWayHandler, NotifyStreamMessage>(this),
                _ => dispatch.Unexpected(messageType)
            };

        public ValueTask DispatchRequest(NotifyMessage message, CancellationToken cancellationToken)
        {
            recorder.Record(message.Value);
            return default;
        }

        // Split out because a ref struct message cannot be a parameter of an async method.
        public ValueTask DispatchRequest(StreamingMessage<NotifyStreamMessage> message,
            CancellationToken cancellationToken) =>
            Drain(message.Header.Value, message.Body, recorder, cancellationToken);

        private static async ValueTask Drain(short value, PipeReader body, OneWayRecorder recorder,
            CancellationToken cancellationToken)
        {
            var buffer = new ArrayBufferWriter<byte>();

            while (true)
            {
                var result = await body.ReadAsync(cancellationToken);
                if (result.Buffer.IsEmpty)
                {
                    break;
                }

                foreach (var segment in result.Buffer)
                {
                    buffer.Write(segment.Span);
                }

                body.AdvanceTo(result.Buffer.End);
            }

            recorder.Record(value, buffer.WrittenSpan.ToArray());
        }
    }

    private readonly struct SilentHandler : IConnectionHandler
    {
        public DispatchResult TryParseAndDispatch(MessageType messageType, ref Dispatcher dispatch) =>
            dispatch.Unexpected(messageType);
    }
}
