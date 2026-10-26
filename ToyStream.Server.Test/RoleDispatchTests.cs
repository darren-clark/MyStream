namespace ToyStream.Server.Test;

using System.IO.Pipelines;
using ToyStream.Protocol;
using ToyStream.Protocol.Messages;
using ToyStream.Protocol.Responses;
using ToyStream.Server.InMemory;
using ToyStream.Shared;

/// <summary>
/// The role decides the switch, so a message outside a role's set is a protocol violation rather
/// than an unimplemented handler. These prove the separation is real and not just documentation.
/// </summary>
public class RoleDispatchTests
{
    [Test]
    public async Task ProducerConnectionCannotCreateATopic()
    {
        var (accepting, client, cancellation) = Connect(ConnectionRole.Producer);
        using var lifetime = cancellation;

        _ = client.WithResult<CreateTopicResult>()
            .Send(new CreateTopicMessage { TopicName = new Utf8String("events"), PartitionCount = 1 },
                cancellation.Token);

        Assert.That(async () => await accepting, Throws.InstanceOf<InvalidOperationException>());
        await cancellation.CancelAsync();
    }

    [Test]
    public async Task AdminConnectionCannotFetch()
    {
        var (accepting, client, cancellation) = Connect(ConnectionRole.Admin);
        using var lifetime = cancellation;

        _ = client.SendMessageForStreamedResponse<FetchRequestMessage, FetchResponseMessage, FetchResponse>(
            new FetchRequestMessage
            {
                MaxWaitMilliseconds = 0,
                MaxBytes = 1024,
                Topics = new TopicNames([new Utf8String("events")]),
                Entries = new FetchEntries([new FetchEntry(0, 0, 0)])
            }, cancellation.Token);

        Assert.That(async () => await accepting, Throws.InstanceOf<InvalidOperationException>());
        await cancellation.CancelAsync();
    }

    /// <summary>A role byte outside the set is refused, so an unknown peer is told rather than hung.</summary>
    [Test]
    public async Task AnUnknownRoleIsRefused()
    {
        var clientToServer = new Pipe();
        var serverToClient = new Pipe();
        using var cancellation = new CancellationTokenSource();

        byte[] handshake = [.. ProtocolHandshake.Magic, ProtocolHandshake.CurrentVersion, 99];
        await clientToServer.Writer.WriteAsync(handshake, cancellation.Token);
        await TestServer.Serving(new InMemoryTopicService())
            .Accept(new PipePair(clientToServer.Reader, serverToClient.Writer), cancellation.Token);

        var refusal = await serverToClient.Reader.ReadAsync(cancellation.Token);
        Assert.That(refusal.Buffer.FirstSpan[0], Is.EqualTo((byte)HandshakeStatus.UnknownRole));
    }

    [Test]
    public async Task ForeignProtocolIsRefusedBeforeAnyHandlerRuns()
    {
        var clientToServer = new Pipe();
        var serverToClient = new Pipe();
        using var cancellation = new CancellationTokenSource();

        await clientToServer.Writer.WriteAsync("GET / HTTP/1.1"u8.ToArray(), cancellation.Token);
        await TestServer.Serving(new InMemoryTopicService())
            .Accept(new PipePair(clientToServer.Reader, serverToClient.Writer), cancellation.Token);

        var refusal = await serverToClient.Reader.ReadAsync(cancellation.Token);
        Assert.That(refusal.Buffer.FirstSpan[0], Is.EqualTo((byte)HandshakeStatus.NotToyStream));
    }

    private static (Task Accepting, ConnectionBase Client, CancellationTokenSource Cancellation) Connect(
        ConnectionRole role)
    {
        var clientToServer = new Pipe();
        var serverToClient = new Pipe();
        var cancellation = new CancellationTokenSource();

        var accepting = TestServer.Serving(new InMemoryTopicService())
            .Accept(new PipePair(clientToServer.Reader, serverToClient.Writer), cancellation.Token);

        ProtocolHandshake.WriteAsync(clientToServer.Writer, role, cancellation.Token).AsTask().Wait();

        var client = new Connection<NoRequestHandler>(
            serverToClient.Reader, clientToServer.Writer, new NoRequestHandler());
        _ = client.Start(cancellation.Token);

        return (accepting, client, cancellation);
    }

    private readonly struct NoRequestHandler : IConnectionHandler
    {
        public DispatchResult TryParseAndDispatch(MessageType messageType, ref Dispatcher dispatch) =>
            dispatch.Unexpected(messageType);
    }
}
