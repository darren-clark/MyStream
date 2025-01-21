namespace ToyStream.Protocol.Benchmark;

using System.Diagnostics;
using System.IO.Pipelines;
using BenchmarkDotNet.Attributes;
using Messages;
using Responses;
using Shared;

[MemoryDiagnoser]
[DisassemblyDiagnoser]
[ThreadingDiagnoser]
public class Benchmarks
{
    [Params(10, 100, 1000)]
    public int MessageRuns;

    private CancellationTokenSource cancellation;
    private TestClientConnection clientConnection;
    private IAsyncDisposable serverConnection;
    
    // [GlobalSetup]
    // public void SetupFakeServer()
    // {
    //     Console.WriteLine("Setting up server");
    //     // Client connection, reader is reading from the "server"
    //     // writer is writing to the "server"
    //     var clientPipe =  new Pipe();
    //     // Server connection, reader is reading from the "client"
    //     // writer is writing to the "client"
    //     var serverPipe = new Pipe();
    //     cancellation = new CancellationTokenSource();
    //     
    //     clientConnection = new TestClientConnection(new ToyProtocolPipeReader(serverPipe.Reader), new ToyProtocolPipeWriter(clientPipe.Writer), cancellation.Token);
    //     serverConnection = new TestServerConnection(new ToyProtocolPipeReader(clientPipe.Reader), new ToyProtocolPipeWriter(serverPipe.Writer), cancellation.Token);
    //     // Debugger.Launch();
    //     // while (!Debugger.IsAttached)
    //     //     Thread.Sleep(TimeSpan.FromMilliseconds(500));
    // }
    //
    // [GlobalCleanup]
    // public async Task Cleanup()
    // {
    //     Console.WriteLine("Cleaning up");
    //     await this.cancellation.CancelAsync();
    //     await this.serverConnection.DisposeAsync();
    //     Console.WriteLine("Done cleaning up");
    // }
    
    [Benchmark]
    public async Task SimpleMessageFlow()
    {
        // Client connection, reader is reading from the "server"
        // writer is writing to the "server"
        var clientPipe =  new Pipe();
        // Server connection, reader is reading from the "client"
        // writer is writing to the "client"
        var serverPipe = new Pipe();
        cancellation = new CancellationTokenSource();
        
        clientConnection = new TestClientConnection(new ToyProtocolPipeReader(serverPipe.Reader), new ToyProtocolPipeWriter(clientPipe.Writer), cancellation.Token);
        serverConnection = new TestServerConnection(new ToyProtocolPipeReader(clientPipe.Reader), new ToyProtocolPipeWriter(serverPipe.Writer), cancellation.Token);

        var topicName = new Utf8String("test");
        for(var i = 0; i< MessageRuns; i++)
        {
            await clientConnection.CreateTopic(topicName, 1).WaitAsync(TimeSpan.FromSeconds(5));
        }
        
        await this.cancellation.CancelAsync();
        await this.serverConnection.DisposeAsync();
    }

    
    // All we do is simply ack the request.
    private class TestServerConnection : ConnectionBase
    {
        public TestServerConnection(IToyProtocolReader reader, IToyProtocolWriter writer, CancellationToken cancellationToken) : base(reader, writer, cancellationToken, "server")
        {
        }

        protected override ValueTask HandleCreateTopicMessage(ref readonly CreateTopicMessage message)
        {
            Log("Received create topic message");
            return SendResponse(new AcknowledgeMessage());
        }
    }
    
    private class TestClientConnection : ConnectionBase
    {
        public TestClientConnection(IToyProtocolReader reader, IToyProtocolWriter writer, CancellationToken cancellationToken) : base(reader, writer, cancellationToken, "client")
        {
        }

        public Task CreateTopic(RefUtf8String topicName, short partitionCount)
        {
            return SendMessage<CreateTopicMessage, AcknowledgeMessage>(new CreateTopicMessage
            {
                TopicName = topicName,
                PartitionCount = partitionCount
            }, default);
        }
    }    
}