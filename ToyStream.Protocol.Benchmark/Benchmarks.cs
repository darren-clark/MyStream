namespace ToyStream.Protocol.Benchmark;

using System.IO.Pipelines;
using BenchmarkDotNet.Attributes;
using Messages;
using Responses;
using Shared;

[MemoryDiagnoser]
[ThreadingDiagnoser]
public class Benchmarks
{
    // Amortises BenchmarkDotNet's per-invocation overhead; results are reported per round trip.
    private const int RoundTrips = 1000;

    private CancellationTokenSource cancellation = null!;
    private TestClientConnection client = null!;
    private IAsyncDisposable clientDisposable = null!;
    private IAsyncDisposable serverDisposable = null!;
    private Utf8String topicName = null!;

    [GlobalSetup]
    public void Setup()
    {
        // Client reads what the server writes, and writes what the server reads.
        var clientPipe = new Pipe();
        var serverPipe = new Pipe();

        this.cancellation = new CancellationTokenSource();
        this.topicName = new Utf8String("test");

        var clientConnection = new TestClientConnection(
            new ToyProtocolPipeReader(serverPipe.Reader),
            new ToyProtocolPipeWriter(clientPipe.Writer),
            this.cancellation.Token);

        var serverConnection = new TestServerConnection(
            new ToyProtocolPipeReader(clientPipe.Reader),
            new ToyProtocolPipeWriter(serverPipe.Writer),
            this.cancellation.Token);

        this.client = clientConnection;
        this.clientDisposable = clientConnection;
        this.serverDisposable = serverConnection;
    }

    [GlobalCleanup]
    public async Task Cleanup()
    {
        await this.cancellation.CancelAsync();
        await this.serverDisposable.DisposeAsync();
        await this.clientDisposable.DisposeAsync();
        this.cancellation.Dispose();
    }

    [Benchmark(OperationsPerInvoke = RoundTrips, Baseline = true)]
    public async Task RequestResponseRoundTrip()
    {
        for (var i = 0; i < RoundTrips; i++)
        {
            await this.client.CreateTopic(this.topicName, 1);
        }
    }

    // All we do is simply ack the request.
    private sealed class TestServerConnection : ConnectionBase
    {
        public TestServerConnection(IToyProtocolReader reader, IToyProtocolWriter writer, CancellationToken cancellationToken)
            : base(reader, writer, cancellationToken, "server")
        {
        }

        protected override ValueTask HandleCreateTopicMessage(ref readonly CreateTopicMessage message)
        {
            Log("Received create topic message");
            return SendResponse(new AcknowledgeMessage());
        }
    }

    private sealed class TestClientConnection : ConnectionBase
    {
        public TestClientConnection(IToyProtocolReader reader, IToyProtocolWriter writer, CancellationToken cancellationToken)
            : base(reader, writer, cancellationToken, "client")
        {
        }

        public ValueTask<AcknowledgeResponse> CreateTopic(RefUtf8String topicName, short partitionCount)
            => SendMessage<CreateTopicMessage, AcknowledgeMessage, AcknowledgeResponse>(
                new CreateTopicMessage
                {
                    TopicName = topicName,
                    PartitionCount = partitionCount
                },
                default);
    }
}
