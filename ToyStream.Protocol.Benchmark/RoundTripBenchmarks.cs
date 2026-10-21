
namespace ToyStream.Protocol.Benchmark;

using System.Buffers;
using System.IO.Pipelines;
using BenchmarkDotNet.Attributes;
using ToyStream.Protocol;
using ToyStream.Protocol.Messages;
using ToyStream.Protocol.Responses;
using ToyStream.Shared;

/// <summary>
/// Regression guard for the shipped design: a request/response round trip must stay allocation free
/// and around 420 ns. Run with --job Long; the default job cannot resolve changes under ~5%.
/// </summary>
[MemoryDiagnoser]
public class RoundTripBenchmarks
{
    private const int RoundTrips = 1000;

    private CancellationTokenSource cancellation = null!;
    private Utf8String topicName = null!;
    private Connection<BenchmarkClientHandler> client = null!;

    [GlobalSetup]
    public void Setup()
    {
        this.cancellation = new CancellationTokenSource();
        this.topicName = new Utf8String("test");

        var options = new PipeOptions(
            readerScheduler: PipeScheduler.Inline,
            writerScheduler: PipeScheduler.Inline,
            useSynchronizationContext: false);

        var clientToServer = new Pipe(options);
        var serverToClient = new Pipe(options);

        var server = new Connection<BenchmarkServerHandler>(
            clientToServer.Reader, serverToClient.Writer, new BenchmarkServerHandler());

        this.client = new Connection<BenchmarkClientHandler>(
            serverToClient.Reader, clientToServer.Writer, new BenchmarkClientHandler());

        // Start returns only when the read loop ends, so it is deliberately not awaited here.
        _ = server.Start(this.cancellation.Token);
        _ = this.client.Start(this.cancellation.Token);
    }

    [GlobalCleanup]
    public async Task Cleanup()
    {
        await this.cancellation.CancelAsync();
        this.cancellation.Dispose();
    }

    [Benchmark(OperationsPerInvoke = RoundTrips)]
    public async Task RoundTrip()
    {
        for (var i = 0; i < RoundTrips; i++)
        {
            await this.client.SendMessage<CreateTopicMessage, AcknowledgeMessage<CreateTopicResult>, AcknowledgeResponse<CreateTopicResult>>(
                new CreateTopicMessage { TopicName = this.topicName, PartitionCount = 1 },
                default);
        }
    }
}
