namespace ToyStream.Protocol.Test;

using System.IO.Pipelines;
using System.Threading.Channels;
using Client;
using ReceiveRef;
using Send;
using Shared;
using AcknowledgeMessage = Receive.AcknowledgeMessage;
using IMessage = Receive.IMessage;
using IToyProtocolHandler = Receive.IToyProtocolHandler;
using IToyProtocolReader = Receive.IToyProtocolReader;
using ToyProtocolReader = Receive.ToyProtocolReader;

public class Tests
{
    [SetUp]
    public void Setup()
    {
    }

    [Test]
    public async Task Test1()
    {
        // Client connection, reader is reading from the "server"
        // writer is writing to the "server"
        var clientPipe =  new Pipe();
        // Server connection, reader is reading from the "client"
        // writer is writing to the "client"
        var serverPipe = new Pipe();
        var cancellation = new CancellationTokenSource();
        var clientConnection = new TestClientConnection(new ToyProtocolReader(serverPipe.Reader, "Client"), new ToyProtocolWriter(clientPipe.Writer), cancellation.Token);
        
        // Writer for our "server". Writes to the pipe the client is reading.
        var serverWriter = new ToyProtocolWriter(serverPipe.Writer);
        // Reader for our "server". Reads from the pipe the client is writing to.
        var serverReader = new ToyProtocolReader(clientPipe.Reader, "Server");
        // Handler for our "server"
        var serverHandler = new DebugHandler();
        // Start the server
        var serverTask = serverReader.Read(serverHandler, cancellation.Token);

        // Ugh, this is brutal.
        // Ok, here's what we're doing.
        // 1. We're going call our CreateTopic method, but NOT await. It won't return until it gets an acknowledgement.
        // 2. We're going to send an acknowledgement message to the client, as if we were the server.
        // 3. We're going to await the CreateTopic method. This should now return because we've acknowledged the message.
        // 4. We're going to await the serverHandler's task, which is the task that gets set when the serverHandler receives a message. This should have already been signaled.
        // 5. We're going to cancel the token, which will cause the "server" to stop reading.
        // 6. We're going to await the serverTask, which should throw an OperationCanceledException.
        var testTask = clientConnection.CreateTopic("Topic", 4);
        await serverWriter.Write(new Protocol.Send.AcknowledgeMessage(), default);
        await testTask;
        var message = await serverHandler.Task;
        await cancellation.CancelAsync();
        try
        {
            await serverTask;
        }
        catch (OperationCanceledException)
        {
        }
    }

    private class DebugHandler : IToyProtocolHandler
    {
        private readonly TaskCompletionSource<IMessage> tcs = new TaskCompletionSource<IMessage>();
        
        public ValueTask HandleMessage<T>(ref readonly T message) where T : struct, IMessage
        {
            Console.WriteLine(message.ToString());
            this.tcs.TrySetResult(message);
            return ValueTask.CompletedTask;
        }

        public Task<IMessage> Task => this.tcs.Task;
    }
    
    private class TestClientConnection : ConnectionBase
    {
        public TestClientConnection(IToyProtocolReader reader, IToyProtocolWriter writer, CancellationToken cancellationToken) : base(reader, writer, cancellationToken)
        {
        }

        public async Task CreateTopic(string topicName, short partitionCount)
        {
            var message = new Send.CreateTopicMessage(topicName, partitionCount);
            await SendMessage<Send.CreateTopicMessage, AcknowledgeMessage>(message, default);
        }

        protected override ValueTask HandleMessageAsync<T>(ref readonly T message)
        {
            return ValueTask.CompletedTask;
        }
    }
}