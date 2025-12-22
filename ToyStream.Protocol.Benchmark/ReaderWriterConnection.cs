namespace ToyStream.Protocol.Benchmark;

using Messages;
using ReaderWriterVirtual;
using Shared;

// All we do is simply ack the request.
public class TestServerVirtualReaderWriterConnection : VirtualReaderWriterConnectionBase
{
    public TestServerVirtualReaderWriterConnection(IToyProtocolReader reader, IToyProtocolWriter writer, CancellationToken cancellationToken) : base(reader, writer, cancellationToken, "server")
    {
    }

    protected override ValueTask HandleCreateTopicMessage(ref readonly CreateTopicMessage message)
    {
        return SendResponse(new CreateTopicResponseMessage() { Result = CreateTopicResult.TopicCreated });
    }
}

public class TestClientVirtualReaderWriterConnection : VirtualReaderWriterConnectionBase
{
    public TestClientVirtualReaderWriterConnection(IToyProtocolReader reader, IToyProtocolWriter writer, CancellationToken cancellationToken) : base(reader, writer, cancellationToken, "client")
    {
    }

    public Task<CreateTopicResponse> CreateTopic(RefUtf8String topicName, short partitionCount)
    {
        return SendMessage<CreateTopicMessage, CreateTopicResponseMessage, CreateTopicResponse>(new CreateTopicMessage
        {
            TopicName = topicName,
            PartitionCount = partitionCount
        }, default);
    }
}