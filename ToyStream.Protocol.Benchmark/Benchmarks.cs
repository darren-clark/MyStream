namespace ToyStream.Protocol.Benchmark;

using System.IO.Pipelines;
using BenchmarkDotNet.Attributes;
using Messages;
using Shared;

[MemoryDiagnoser]
[DisassemblyDiagnoser]
[ThreadingDiagnoser]
public class Benchmarks
{
    [Params(10, 100, 1000)]
    public int MessageRuns;

    [Benchmark(Baseline = true)]
    public async Task ReadWriterConnection()
    {
        // Client connection, reader is reading from the "server"
        // writer is writing to the "server"
        var clientPipe =  new Pipe();
        // Server connection, reader is reading from the "client"
        // writer is writing to the "client"
        var serverPipe = new Pipe();
        var cancellation = new CancellationTokenSource();
        
        var clientReaderWriterConnection = new TestClientVirtualReaderWriterConnection(new ToyProtocolPipeReader(serverPipe.Reader), new ToyProtocolPipeWriter(clientPipe.Writer), cancellation.Token);
        await using var serverConnection = new TestServerVirtualReaderWriterConnection(new ToyProtocolPipeReader(clientPipe.Reader), new ToyProtocolPipeWriter(serverPipe.Writer), cancellation.Token);

        var topicName = new Utf8String("test");
        for(var i = 0; i< MessageRuns; i++)
        {
            await clientReaderWriterConnection.CreateTopic(topicName, 1).WaitAsync(TimeSpan.FromSeconds(5));
        }
        
        await cancellation.CancelAsync();
    }

    
    
    
}