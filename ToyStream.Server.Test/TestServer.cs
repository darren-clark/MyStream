namespace ToyStream.Server.Test;

using System.IO.Pipelines;
using Microsoft.Extensions.DependencyInjection;
using ToyStream.Protocol;

/// <summary>A transport that is already connected, which is all a pipe-level test needs.</summary>
internal sealed class PipePair(PipeReader input, PipeWriter output) : ITransportConnection
{
    public PipeReader Input => input;

    public PipeWriter Output => output;

    /// <summary>
    /// Completes both halves rather than doing nothing, so a peer going away looks the way a closed
    /// socket does. A subscriber's group membership is a connection, so it depends on this.
    /// </summary>
    public async ValueTask DisposeAsync()
    {
        await input.CompleteAsync();
        await output.CompleteAsync();
    }
}

internal static class TestServer
{
    public static ConnectionAcceptor Serving(ITopicService topics) =>
        Serving(topics, new InMemory.InMemoryConsumerGroupService());

    public static ConnectionAcceptor Serving(ITopicService topics, IConsumerGroupService groups,
        ConsumerGroupCoordinator? coordinator = null) =>
        new(new ServiceCollection()
            .AddSingleton(topics)
            .AddSingleton(groups)
            .AddSingleton(coordinator ?? new ConsumerGroupCoordinator(groups))
            .BuildServiceProvider());
}
