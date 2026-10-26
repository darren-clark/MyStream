namespace ToyStream.Server.Test;

using System.IO.Pipelines;
using Microsoft.Extensions.DependencyInjection;
using ToyStream.Protocol;
using ToyStream.Protocol.Messages;
using ToyStream.Protocol.Responses;
using ToyStream.Server.InMemory;
using ToyStream.Shared;

/// <summary>
/// Handlers are built per connection by <c>ActivatorUtilities</c>, so a missing registration is a
/// runtime failure on first connect rather than a build error. This runs the host's own composition
/// instead of a hand-built one, which is the only thing that proves the registrations line up.
/// </summary>
public class HostCompositionTests
{
    [Test]
    public async Task TheHostsRegistrationsServeARoundTrip()
    {
        var services = new ServiceCollection();
        services.AddInMemoryServiceProviders();

        var acceptor = new ConnectionAcceptor(services.BuildServiceProvider());

        var clientToServer = new Pipe();
        var serverToClient = new Pipe();
        using var cancellation = new CancellationTokenSource();

        var accepting = acceptor.Accept(
            new PipePair(clientToServer.Reader, serverToClient.Writer), cancellation.Token);

        await ProtocolHandshake.WriteAsync(clientToServer.Writer, ConnectionRole.Admin, cancellation.Token);

        var client = new Connection<NoRequestHandler>(
            serverToClient.Reader, clientToServer.Writer, new NoRequestHandler());
        _ = client.Start(cancellation.Token);

        var send = client.WithResult<CreateTopicResult>()
            .Send(new CreateTopicMessage { TopicName = new Utf8String("events"), PartitionCount = 2 },
                cancellation.Token)
            .AsTask();

        // A failed resolve kills the accept rather than answering, so surface that instead of
        // waiting out a response that is never coming.
        if (await Task.WhenAny(send, accepting).ConfigureAwait(false) == accepting)
        {
            await accepting;
        }

        Assert.That(await send, Is.EqualTo(CreateTopicResult.Success));

        await cancellation.CancelAsync();
    }

    private readonly struct NoRequestHandler : IConnectionHandler
    {
        public DispatchResult TryParseAndDispatch(MessageType messageType, ref Dispatcher dispatch) =>
            dispatch.Unexpected(messageType);
    }
}
