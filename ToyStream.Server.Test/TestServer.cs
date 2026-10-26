namespace ToyStream.Server.Test;

using System.IO.Pipelines;
using Microsoft.Extensions.DependencyInjection;
using ToyStream.Protocol;

/// <summary>A transport that is already connected, which is all a pipe-level test needs.</summary>
internal sealed class PipePair(PipeReader input, PipeWriter output) : ITransportConnection
{
    public PipeReader Input => input;

    public PipeWriter Output => output;

    public ValueTask DisposeAsync() => default;
}

internal static class TestServer
{
    public static ConnectionAcceptor Serving(ITopicService topics) =>
        new(new ServiceCollection().AddSingleton(topics).BuildServiceProvider());
}
