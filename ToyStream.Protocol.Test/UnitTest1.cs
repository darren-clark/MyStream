namespace ToyStream.Protocol.Test;

using System.IO.Pipelines;
using Protocol.Messages;
using ReaderWriterVirtual;
using Shared;

// using AcknowledgeMessage = Receive.AcknowledgeMessage;
// using IMessage = Receive.IMessage;
// using IToyProtocolHandler = Receive.IToyProtocolHandler;
// using IToyProtocolReader = Receive.IToyProtocolReader;
// using ToyProtocolReader = Receive.ToyProtocolReader;

public class Tests
{
    [SetUp]
    public void Setup()
    {
    }

    [Test]
    public async Task TestClientWait()
    {
        // Client connection, reader is reading from the "server"
        // writer is writing to the "server"
        var clientPipe = new Pipe();
        // Server connection, reader is reading from the "client"
        // writer is writing to the "client"
        var serverPipe = new Pipe();
        var cancellation = new CancellationTokenSource();
        var clientConnection = new TestClientConnection(new ToyProtocolPipeReader(serverPipe.Reader, "Client"),
            new ToyProtocolPipeWriter(clientPipe.Writer), cancellation.Token);

        // Writer for our "server". Writes to the pipe the client is reading.
        var serverWriter = new ToyProtocolPipeWriter(serverPipe.Writer);
        // Reader for our "server". Reads from the pipe the client is writing to.
        var serverReader = new ToyProtocolPipeReader(clientPipe.Reader, "Server");
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
        await serverWriter.Write(new CreateTopicResponseMessage { Result = CreateTopicResult.TopicCreated }, default);
        await testTask;
        testTask = clientConnection.CreateTopic("Topic", 4);
        await serverWriter.Write(new CreateTopicResponseMessage { Result = CreateTopicResult.DuplicateTopic }, default);
        await testTask;
        await serverHandler.Task;
        await cancellation.CancelAsync();
        try
        {
            await serverTask;
        }
        catch (OperationCanceledException)
        {
        }
    }

    [Test]
    public async Task TestServerRoundTrip()
    {
        // Client connection, reader is reading from the "server"
        // writer is writing to the "server"
        var clientPipe = new Pipe();
        // Server connection, reader is reading from the "client"
        // writer is writing to the "client"
        var serverPipe = new Pipe();
        var cancellation = new CancellationTokenSource();
        var clientConnection = new TestClientConnection(new ToyProtocolPipeReader(serverPipe.Reader, "Client"),
            new ToyProtocolPipeWriter(clientPipe.Writer), cancellation.Token);
        await using var _ = new TestServerVirtualReaderWriterConnection(new ToyProtocolPipeReader(clientPipe.Reader),
            new ToyProtocolPipeWriter(serverPipe.Writer), cancellation.Token);
        for (var i = 0; i < 100; i++)
        {
            Console.WriteLine($"Attempt {i}");
            await clientConnection.CreateTopic("test", 1);
        }

        await cancellation.CancelAsync();
    }

    // All we do is simply ack the request.
    private class TestServerVirtualReaderWriterConnection : VirtualReaderWriterConnectionBase
    {
        public TestServerVirtualReaderWriterConnection(IToyProtocolReader reader, IToyProtocolWriter writer,
            CancellationToken cancellationToken) : base(reader, writer, cancellationToken)
        {
        }

        protected override ValueTask HandleCreateTopicMessage(ref readonly CreateTopicMessage message)
        {
            return SendResponse(new CreateTopicResponseMessage()
            {
                Result = CreateTopicResult.TopicCreated
            });
        }
    }

    private class DebugHandler : IToyProtocolHandler
    {
        private readonly TaskCompletionSource tcs = new TaskCompletionSource();

        public Task Task => this.tcs.Task;


        public ValueTask HandleCreateTopicMessage(ref readonly CreateTopicMessage message)
        {
            this.tcs.TrySetResult();
            return default;
        }

        public ValueTask HandleResponseMessage<TResponseMessage, TResponse>(ref readonly TResponseMessage message) where TResponseMessage : struct, IResponseMessage<TResponseMessage>, allows ref struct where TResponse : class, IResponse<TResponseMessage, TResponse>
        {
            return default;
        }
    }

    private class TestClientConnection : VirtualReaderWriterConnectionBase
    {
        public TestClientConnection(IToyProtocolReader reader, IToyProtocolWriter writer,
            CancellationToken cancellationToken) : base(reader, writer, cancellationToken)
        {
        }

        public async Task CreateTopic(string topicName, short partitionCount)
        {
            var message = new CreateTopicMessage
            {
                TopicName = new Utf8String(topicName),
                PartitionCount = partitionCount
            };
            await SendMessage<CreateTopicMessage, CreateTopicResponseMessage, CreateTopicResponse>(message, default);
        }
    }
}