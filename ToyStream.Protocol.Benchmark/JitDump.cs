namespace ToyStream.Protocol.Benchmark;

using System.IO.Pipelines;
using Messages;
using Protocol.Dispatch.Context;
using Responses;
using Shared;

/// <summary>
/// Standalone driver for DOTNET_JitDisasm. Under BenchmarkDotNet the read loop runs on its own task,
/// so it never appears in the disassembly walked from the benchmark method.
/// </summary>
internal static class JitDump
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

        var server = new ContextConnection<ServerContext>(
            clientToServer.Reader, serverToClient.Writer, new ServerContext(new NoopTopicService()));

        var tracker = new ResponseTracker(clientToServer.Writer);
        var client = new ContextConnection<ClientContext>(
            serverToClient.Reader, clientToServer.Writer, new ClientContext(tracker));

        _ = server.Start(cancellation.Token);
        _ = client.Start(cancellation.Token);

        var topicName = new Utf8String("test");
        for (var i = 0; i < 100_000; i++)
        {
            await tracker.SendMessage<CreateTopicMessage, AcknowledgeMessage, AcknowledgeResponse>(
                new CreateTopicMessage { TopicName = topicName, PartitionCount = 1 },
                default);
        }

        await cancellation.CancelAsync();
        Console.WriteLine("jitdump: completed 100000 round trips");
    }

    private sealed class NoopTopicService : ITopicService
    {
        public ValueTask CreateTopic(RefUtf8String topicName, short partitionCount) => default;
    }
}
