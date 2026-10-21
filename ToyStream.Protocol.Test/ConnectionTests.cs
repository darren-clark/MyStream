
namespace ToyStream.Protocol.Test;

using System.IO.Pipelines;
using ToyStream.Protocol.Messages;
using ToyStream.Protocol.Responses;
using ToyStream.Shared;

using static Messages.ProtocolTestHelpers;

public class ConnectionBaseTests
{
    private static (Pipe ClientToServer, Pipe ServerToClient) CreatePipes() => (new Pipe(), new Pipe());

    private static CreateTopicMessage CreateTopic(string name, short partitions) => new()
    {
        TopicName = new Utf8String(name),
        PartitionCount = partitions,
    };

    /// <summary>
    /// The send must stay pending until the peer answers - this is what the completion source cache
    /// exists for, and it is the part most easily broken by a change to the read loop.
    /// </summary>
    [Test]
    public async Task SendStaysPendingUntilResponseArrives()
    {
        var (clientToServer, serverToClient) = CreatePipes();
        using var cancellation = new CancellationTokenSource();

        var client = new Connection<ClientTestHandler>(
            serverToClient.Reader, clientToServer.Writer, new ClientTestHandler());
        _ = client.Start(cancellation.Token);

        var send = client.SendMessage<CreateTopicMessage, AcknowledgeMessage<CreateTopicResult>, AcknowledgeResponse<CreateTopicResult>>(
            CreateTopic("topic", 4), default);

        Assert.That(send.IsCompleted, Is.False, "send completed before the peer acknowledged");

        // Act as the server and acknowledge.
        var serverWriter = new PrimitiveWriter(serverToClient.Writer);
        new ResponseWriter<AcknowledgeMessage<CreateTopicResult>>(serverWriter)
            .WriteResponse(AcknowledgeMessage.WithResult(CreateTopicResult.Success));
        await serverToClient.Writer.FlushAsync(cancellation.Token);

        Assert.That((await send).Result, Is.EqualTo(CreateTopicResult.Success));
        await cancellation.CancelAsync();
    }

    [Test]
    public async Task ServerRoundTripRepeats()
    {
        var (clientToServer, serverToClient) = CreatePipes();
        using var cancellation = new CancellationTokenSource();
        var firstRequest = new TaskCompletionSource();

        var server = new Connection<ServerTestHandler>(
            clientToServer.Reader, serverToClient.Writer, new ServerTestHandler(firstRequest));

        var client = new Connection<ClientTestHandler>(
            serverToClient.Reader, clientToServer.Writer, new ClientTestHandler());

        // Start returns only when the read loop ends, so it is deliberately not awaited here.
        _ = server.Start(cancellation.Token);
        _ = client.Start(cancellation.Token);

        for (var i = 0; i < 100; i++)
        {
            var response = await client.SendMessage<CreateTopicMessage, AcknowledgeMessage<CreateTopicResult>, AcknowledgeResponse<CreateTopicResult>>(
                CreateTopic("test", 1), default);

            Assert.That(response.Result, Is.EqualTo(CreateTopicResult.Success), $"round trip {i} returned the wrong result");
        }

        await firstRequest.Task;
        await cancellation.CancelAsync();
    }

    /// <summary>
    /// The response slot and the writer are single-occupancy, so a second send must fail loudly
    /// rather than overwrite the pending completion and hang the first caller.
    /// </summary>
    [Test]
    public async Task SecondSendWhileOneIsPendingThrows()
    {
        var (clientToServer, serverToClient) = CreatePipes();
        using var cancellation = new CancellationTokenSource();

        var client = new Connection<ClientTestHandler>(
            serverToClient.Reader, clientToServer.Writer, new ClientTestHandler());
        _ = client.Start(cancellation.Token);

        var send = client.WithResult<CreateTopicResult>().Send(CreateTopic("first", 1), default);
        Assert.That(send.IsCompleted, Is.False);

        Assert.Throws<InvalidOperationException>(() =>
            _ = client.WithResult<CreateTopicResult>().Send(CreateTopic("second", 1), default));

        // The first request is untouched by the rejected one.
        var serverWriter = new PrimitiveWriter(serverToClient.Writer);
        new ResponseWriter<AcknowledgeMessage<CreateTopicResult>>(serverWriter)
            .WriteResponse(AcknowledgeMessage.WithResult(CreateTopicResult.Success));
        await serverToClient.Writer.FlushAsync(cancellation.Token);

        Assert.That(await send, Is.EqualTo(CreateTopicResult.Success));
        await cancellation.CancelAsync();
    }

    /// <summary>The guard must release on every path, or one cancelled request wedges the connection.</summary>
    [Test]
    public async Task CancelledSendReleasesTheConnectionForReuse()
    {
        var (clientToServer, serverToClient) = CreatePipes();
        using var cancellation = new CancellationTokenSource();

        var server = new Connection<ServerTestHandler>(
            clientToServer.Reader, serverToClient.Writer, new ServerTestHandler(new TaskCompletionSource()));
        var client = new Connection<ClientTestHandler>(
            serverToClient.Reader, clientToServer.Writer, new ClientTestHandler());

        _ = client.Start(cancellation.Token);

        using var requestCancellation = new CancellationTokenSource();
        var send = client.WithResult<CreateTopicResult>().Send(CreateTopic("abandoned", 1), requestCancellation.Token);
        await requestCancellation.CancelAsync();
        Assert.That(async () => await send, Throws.InstanceOf<OperationCanceledException>());

        // Only now start the peer, so the abandoned request is answered after it was cancelled.
        _ = server.Start(cancellation.Token);

        var result = await client.WithResult<CreateTopicResult>().Send(CreateTopic("next", 1), cancellation.Token);
        Assert.That(result, Is.EqualTo(CreateTopicResult.Success));
        await cancellation.CancelAsync();
    }

    /// <summary>
    /// The read loop drains every message a single read delivered before flushing, so a pipelined
    /// batch costs one flush. Handling them one read at a time would still answer them all, so the
    /// flush count is what actually distinguishes the two.
    /// </summary>
    [Test]
    public async Task PipelinedRequestsInOneReadCostOneFlush()
    {
        var (clientToServer, serverToClient) = CreatePipes();
        using var cancellation = new CancellationTokenSource();
        var counter = new RequestCounter(target: 3);

        // Buffered before the server starts, so all three arrive in one read.
        var writer = new PrimitiveWriter(clientToServer.Writer);
        for (var i = 0; i < 3; i++)
        {
            writer.Write((byte)MessageType.CreateTopic);
            WriteMessage(CreateTopic($"topic-{i}", 1), writer);
        }

        await clientToServer.Writer.FlushAsync(cancellation.Token);

        var serverOutput = new FlushRecordingWriter(serverToClient.Writer, counter);
        var server = new Connection<CountingServerHandler>(
            clientToServer.Reader, serverOutput, new CountingServerHandler(counter));
        _ = server.Start(cancellation.Token);

        var dispatchedAtFirstFlush = await serverOutput.DispatchedAtFirstFlush
            .WaitAsync(TimeSpan.FromSeconds(10), cancellation.Token);

        Assert.That(dispatchedAtFirstFlush, Is.EqualTo(3), "the loop flushed before draining the batch");
        Assert.That(counter.Count, Is.EqualTo(3));
        await cancellation.CancelAsync();
    }

    /// <summary>A type no handler serves must fault the loop rather than be skipped as padding.</summary>
    [Test]
    public async Task UnexpectedMessageTypeFaultsTheReadLoop()
    {
        var (clientToServer, serverToClient) = CreatePipes();
        using var cancellation = new CancellationTokenSource();

        // High bit clear, so the connection routes it to the handler rather than the response path.
        var writer = new PrimitiveWriter(clientToServer.Writer);
        writer.Write((byte)0x7F);
        await clientToServer.Writer.FlushAsync(cancellation.Token);

        var server = new Connection<ServerTestHandler>(
            clientToServer.Reader, serverToClient.Writer, new ServerTestHandler(new TaskCompletionSource()));
        var readLoop = server.Start(cancellation.Token);

        Assert.That(async () => await readLoop, Throws.InstanceOf<InvalidOperationException>());
        await cancellation.CancelAsync();
    }

    private readonly struct ServerTestHandler(TaskCompletionSource received)
        : IConnectionHandler, IRequestHandler<CreateTopicMessage, AcknowledgeMessage<CreateTopicResult>>
    {
        public DispatchResult TryParseAndDispatch(MessageType messageType, ref Dispatcher dispatch) =>
            messageType switch
            {
                MessageType.CreateTopic =>
                    dispatch.Request<ServerTestHandler, CreateTopicMessage, AcknowledgeMessage<CreateTopicResult>>(this),
                _ => dispatch.Unexpected(messageType)
            };

        public ValueTask DispatchRequest(CreateTopicMessage message,
            ResponseWriter<AcknowledgeMessage<CreateTopicResult>> responseWriter, CancellationToken cancellationToken)
        {
            received.TrySetResult();
            responseWriter.WriteResponse(AcknowledgeMessage.WithResult(CreateTopicResult.Success));
            return default;
        }
    }

    private readonly struct ClientTestHandler : IConnectionHandler
    {
        public DispatchResult TryParseAndDispatch(MessageType messageType, ref Dispatcher dispatch) =>
            dispatch.Unexpected(messageType);
    }

    private sealed class RequestCounter(int target)
    {
        private readonly TaskCompletionSource reached = new();
        private int count;

        public int Count => Volatile.Read(ref this.count);

        public Task Reached => this.reached.Task;

        public void Record()
        {
            if (Interlocked.Increment(ref this.count) >= target)
            {
                this.reached.TrySetResult();
            }
        }
    }

    /// <summary>Samples the dispatch count inside the flush, so the reading cannot race the loop.</summary>
    private sealed class FlushRecordingWriter(PipeWriter inner, RequestCounter counter) : PipeWriter
    {
        private readonly TaskCompletionSource<int> firstFlush = new();

        public Task<int> DispatchedAtFirstFlush => this.firstFlush.Task;

        public override ValueTask<FlushResult> FlushAsync(CancellationToken cancellationToken = default)
        {
            this.firstFlush.TrySetResult(counter.Count);
            return inner.FlushAsync(cancellationToken);
        }

        public override void Advance(int bytes) => inner.Advance(bytes);

        public override Memory<byte> GetMemory(int sizeHint = 0) => inner.GetMemory(sizeHint);

        public override Span<byte> GetSpan(int sizeHint = 0) => inner.GetSpan(sizeHint);

        public override void CancelPendingFlush() => inner.CancelPendingFlush();

        public override void Complete(Exception? exception = null) => inner.Complete(exception);
    }

    private readonly struct CountingServerHandler(RequestCounter counter)
        : IConnectionHandler, IRequestHandler<CreateTopicMessage, AcknowledgeMessage<CreateTopicResult>>
    {
        public DispatchResult TryParseAndDispatch(MessageType messageType, ref Dispatcher dispatch) =>
            messageType switch
            {
                MessageType.CreateTopic =>
                    dispatch.Request<CountingServerHandler, CreateTopicMessage,
                        AcknowledgeMessage<CreateTopicResult>>(this),
                _ => dispatch.Unexpected(messageType)
            };

        public ValueTask DispatchRequest(CreateTopicMessage message,
            ResponseWriter<AcknowledgeMessage<CreateTopicResult>> responseWriter, CancellationToken cancellationToken)
        {
            counter.Record();
            responseWriter.WriteResponse(AcknowledgeMessage.WithResult(CreateTopicResult.Success));
            return default;
        }
    }
}
