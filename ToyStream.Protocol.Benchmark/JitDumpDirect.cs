namespace ToyStream.Protocol.Benchmark;

using System.IO.Pipelines;
using Messages;
using Protocol.Dispatch.Direct;
using Responses;
using Shared;

/// <summary>Direct equivalent of <see cref="JitDump"/>, so the two read loops can be compared.</summary>
internal static class JitDumpDirect
{
    public static async Task RunAsync()
    {
        var options = new PipeOptions(
            readerScheduler: PipeScheduler.Inline,
            writerScheduler: PipeScheduler.Inline,
            useSynchronizationContext: false);

        var clientToServer = new Pipe(options);
        var serverToClient = new Pipe(options);
        using var cancellation = new CancellationTokenSource();

        var server = new DirectServerConnection(
            clientToServer.Reader, serverToClient.Writer, new NoopTopicService(), cancellation.Token);
        var client = new DirectClientConnection(
            serverToClient.Reader, clientToServer.Writer, cancellation.Token);

        var topicName = new Utf8String("test");
        for (var i = 0; i < 100_000; i++)
        {
            await client.CreateTopic(topicName, 1);
        }

        await cancellation.CancelAsync();
        GC.KeepAlive(server);
        Console.WriteLine("jitdump direct: completed 100000 round trips");
    }

    private sealed class NoopTopicService : ITopicService
    {
        public ValueTask CreateTopic(RefUtf8String topicName, short partitionCount) => default;
    }
}
