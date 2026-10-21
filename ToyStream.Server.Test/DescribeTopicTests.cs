namespace ToyStream.Server.Test;

using System.Buffers;
using System.Buffers.Binary;
using System.IO.Pipelines;
using ToyStream.Protocol;
using ToyStream.Protocol.Messages;
using ToyStream.Protocol.Responses;
using ToyStream.Server.InMemory;
using ToyStream.Shared;

/// <summary>
/// Describe is entirely synchronous because the descriptions live in stack memory, so these go
/// through the service rather than the wire.
/// </summary>
public class DescribeTopicTests
{
    private static InMemoryTopicService ServiceWithTopic(string name, short partitions)
    {
        var service = new InMemoryTopicService();
        Assert.That(service.TryCreateTopic(new Utf8String(name), partitions).Result, Is.True);
        return service;
    }

    [Test]
    public void DescribeReportsAnEmptyRangePerPartition()
    {
        var service = ServiceWithTopic("events", 3);

        Assert.That(service.TryGetTopic(new Utf8String("events"), out var topic), Is.True);
        Assert.That(topic!.PartitionCount, Is.EqualTo(3));

        Span<PartitionDescription> descriptions = stackalloc PartitionDescription[topic!.PartitionCount];
        topic.Describe(descriptions);

        foreach (var description in descriptions)
        {
            // Half open, so an empty partition is Start == End rather than a sentinel.
            Assert.That(description.StartOffset, Is.EqualTo(0));
            Assert.That(description.EndOffset, Is.EqualTo(0));
        }
    }

    [Test]
    public void UnknownTopicIsNotFound()
    {
        var service = ServiceWithTopic("events", 1);

        Assert.That(service.TryGetTopic(new Utf8String("missing"), out _), Is.False);
    }

    /// <summary>
    /// A describe sizes memory from the partition count, and the bound is a protocol limit, so the
    /// handler is what enforces it rather than any one provider.
    /// </summary>
    [TestCase((short)0)]
    [TestCase((short)-1)]
    [TestCase(short.MinValue)]
    [TestCase(short.MaxValue)]
    public void OutOfRangePartitionCountIsRejectedByTheHandler(short partitionCount)
    {
        var service = new InMemoryTopicService();

        Assert.That(CreateTopic(service, "events", partitionCount),
            Is.EqualTo(CreateTopicResult.InvalidPartitionCount));
        Assert.That(service.TryGetTopic(new Utf8String("events"), out _), Is.False,
            "a rejected topic was still created");
    }

    [TestCase((short)1)]
    [TestCase((short)ProtocolConstants.MaxPartitions)]
    public void PartitionCountAtTheBoundIsAccepted(short partitionCount)
    {
        var service = new InMemoryTopicService();

        Assert.That(CreateTopic(service, "events", partitionCount), Is.EqualTo(CreateTopicResult.Success));
        Assert.That(service.TryGetTopic(new Utf8String("events"), out var topic), Is.True);
        Assert.That(topic!.PartitionCount, Is.EqualTo(partitionCount));
    }

    [Test]
    public void DuplicateTopicIsReported()
    {
        var service = new InMemoryTopicService();

        Assert.That(CreateTopic(service, "events", 2), Is.EqualTo(CreateTopicResult.Success));
        Assert.That(CreateTopic(service, "events", 2), Is.EqualTo(CreateTopicResult.DuplicateTopic));
    }

    /// <summary>The partition id comes off the wire, so a negative one must not index the array.</summary>
    [TestCase((short)-1)]
    [TestCase(short.MinValue)]
    [TestCase((short)4)]
    public void OutOfRangePartitionIdIsRejected(short partitionId)
    {
        var service = ServiceWithTopic("events", 4);
        Assert.That(service.TryGetTopic(new Utf8String("events"), out var topic), Is.True);

        Assert.That(topic!.TryGetPartition(partitionId, out _), Is.False);
    }

    /// <summary>
    /// The handler stack allocates below a threshold and rents above it, so both sides of that
    /// boundary have to describe every partition.
    /// </summary>
    [TestCase((short)1)]
    [TestCase((short)64)]
    [TestCase((short)65)]
    [TestCase((short)ProtocolConstants.MaxPartitions)]
    public void HandlerDescribesEveryPartition(short partitionCount)
    {
        var response = Describe(ServiceWithTopic("events", partitionCount), "events");

        Assert.That((ResponseType)response[0], Is.EqualTo(ResponseType.DescribeTopic));
        Assert.That((DescribeTopicResult)response[1], Is.EqualTo(DescribeTopicResult.Success));
        Assert.That(BinaryPrimitives.ReadInt16BigEndian(response.AsSpan(2)), Is.EqualTo(partitionCount));
        Assert.That(response.Length, Is.EqualTo(4 + (partitionCount * 16)), "a partition was not written");
    }

    [Test]
    public void HandlerReportsAnUnknownTopic()
    {
        var response = Describe(ServiceWithTopic("events", 1), "missing");

        // A failure carries no partition count, so the frame is just the type and the result.
        Assert.That(response.Length, Is.EqualTo(2), $"unexpected response bytes: {Convert.ToHexString(response)}");
        Assert.That((ResponseType)response[0], Is.EqualTo(ResponseType.DescribeTopic));
        Assert.That((DescribeTopicResult)response[1], Is.EqualTo(DescribeTopicResult.UnknownTopic));
    }

    /// <summary>The acknowledge carries an int sized enum after the response type byte.</summary>
    private static CreateTopicResult CreateTopic(ITopicService service, string topicName, short partitionCount)
    {
        var buffer = new TestPipeWriter();
        var responseWriter =
            new ResponseWriter<AcknowledgeMessage<CreateTopicResult>>(new PrimitiveWriter(buffer));

        IRequestHandler<CreateTopicMessage, AcknowledgeMessage<CreateTopicResult>> handler =
            new ServerHandler(service);

        var dispatched = handler.DispatchRequest(
            new CreateTopicMessage { TopicName = new Utf8String(topicName), PartitionCount = partitionCount },
            responseWriter, default);

        Assert.That(dispatched.IsCompletedSuccessfully, Is.True);

        var response = buffer.WrittenSpan.ToArray();
        Assert.That((ResponseType)response[0], Is.EqualTo(ResponseType.AcknowledgeWithResult));
        return (CreateTopicResult)BinaryPrimitives.ReadInt32BigEndian(response.AsSpan(1));
    }

    private static byte[] Describe(ITopicService service, string topicName)    {
        var buffer = new TestPipeWriter();
        var responseWriter = new ResponseWriter<DescribeTopicResponseMessage>(new PrimitiveWriter(buffer));

        IRequestHandler<DescribeTopicRequestMessage, DescribeTopicResponseMessage> handler =
            new ServerHandler(service);

        var dispatched = handler.DispatchRequest(
            new DescribeTopicRequestMessage { TopicName = new Utf8String(topicName) }, responseWriter, default);

        Assert.That(dispatched.IsCompletedSuccessfully, Is.True, "describe should never go async");
        return buffer.WrittenSpan.ToArray();
    }

    private sealed class TestPipeWriter : PipeWriter
    {
        private readonly ArrayBufferWriter<byte> buffer = new(initialCapacity: 1024);

        public ReadOnlySpan<byte> WrittenSpan => this.buffer.WrittenSpan;

        public override void Advance(int bytes) => this.buffer.Advance(bytes);

        public override Memory<byte> GetMemory(int sizeHint = 0) => this.buffer.GetMemory(sizeHint);

        public override Span<byte> GetSpan(int sizeHint = 0) => this.buffer.GetSpan(sizeHint);

        public override void CancelPendingFlush()
        {
        }

        public override void Complete(Exception? exception = null)
        {
        }

        public override ValueTask<FlushResult> FlushAsync(CancellationToken cancellationToken = default) =>
            new(new FlushResult(isCanceled: false, isCompleted: false));
    }
}
