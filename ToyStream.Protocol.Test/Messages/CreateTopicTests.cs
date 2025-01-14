namespace ToyStream.Protocol.Test.Messages;

using System.Buffers;
using Shared;

public class CreateTopicTests
{
    // Creates a buffer containing a serialized CreateTopicMessage with the topic "topic" and partition count 3
    private Span<byte> CreateMessage()
    {
        var writer = new SpanWriter(new byte[1024]);
        new Send.CreateTopicMessage("topic", 3).Write(ref writer);
        return writer.Written;
    }

    // All the read tests assume the buffer is created correctly
    // if it is NOT, then don't bother with them.
    private bool sendPassed = false;
    
    [SetUp]
    public void SetUp()
    {
        if (TestContext.CurrentContext.Test.Name != nameof(this.TestSend))
        {
            Assume.That(this.sendPassed, Is.True);
        }
    }

    [TearDown]
    public void TearDown()
    {
        if (TestContext.CurrentContext.Test.MethodName == nameof(this.TestSend))
        {
            this.sendPassed = TestContext.CurrentContext.Result.Outcome.Status == NUnit.Framework.Interfaces.TestStatus.Passed;
        }
    }
    
    [Test]
    [Order(1)]
    public void TestSend()
    {
        var expectedWriter = new SpanWriter(new byte[1024]);
        new Send.Utf8StringValue("topic").Write(ref expectedWriter);
        new Send.ShortValue(3).Write(ref expectedWriter);
        
        var message = this.CreateMessage();
        Assert.That(message.ToArray(), Is.EqualTo(expectedWriter.Written.ToArray()) );
    }
    
    [Test]
    public void TestReceive_NotEnoughForStringSize()
    {
        // Only 1 byte, not enough for a short(string length)
        var buffer = this.CreateMessage()[..1];
        var reader = new SequenceReader<byte>(new ReadOnlySequence<byte>(buffer.ToArray()));
        Assert.That(Receive.CreateTopicMessage.TryRead(ref reader, out var message, out var size), Is.False);
        Assert.That(reader.Consumed, Is.EqualTo(0));
        Assert.That(message, Is.Null);
        Assert.That(size, Is.EqualTo(1));
    }

    [Test]
    public void TestReceive_NotEnoughForStringPartitionCount()
    {
        // trim one byte from the end, not enough for a short(partition count)
        var buffer = this.CreateMessage()[..^1];
        var reader = new SequenceReader<byte>(new ReadOnlySequence<byte>(buffer.ToArray()));
        Assert.That(Receive.CreateTopicMessage.TryRead(ref reader, out var message, out var size), Is.False);
        Assert.That(reader.Consumed, Is.EqualTo(0));
        Assert.That(message, Is.Null);
        Assert.That(size, Is.EqualTo(1));
    }

    [Test]
    public void TestReceive_Complete()
    {
        var buffer = this.CreateMessage();
        var reader = new SequenceReader<byte>(new ReadOnlySequence<byte>(buffer.ToArray()));
        Assert.That(Receive.CreateTopicMessage.TryRead(ref reader, out var message, out var size), Is.True);
        Assert.That(reader.Consumed, Is.EqualTo(buffer.Length));
        Assert.That(size, Is.EqualTo(0));
        Assert.That(message, Is.Not.Null);
        Assert.That(message.Value.TopicName.Value == "topic");
        Assert.That(message.Value.PartitionCount.Value, Is.EqualTo(3));
    }
    
    [Test]
    public void TestReceiveRef_NotEnoughForStringSize()
    {
        // Only 1 byte, not enough for a short(string length)
        var buffer = this.CreateMessage()[..1];
        var reader = new SequenceReader<byte>(new ReadOnlySequence<byte>(buffer.ToArray()));
        Assert.That(ReceiveRef.CreateTopicMessage.TryRead(reader, out var message, out var size), Is.False);
        Assert.That(size, Is.EqualTo(1));
    }

    [Test]
    public void TestReceiveRef_NotEnoughForStringPartitionCount()
    {
        // trim one byte from the end, not enough for a short(partition count)
        var buffer = this.CreateMessage()[..^1];
        var reader = new SequenceReader<byte>(new ReadOnlySequence<byte>(buffer.ToArray()));
        Assert.That(ReceiveRef.CreateTopicMessage.TryRead(reader, out var message, out var size), Is.False);
        Assert.That(size, Is.EqualTo(1));
    }

    [Test]
    public void TestReceiveRef_Complete()
    {
        var buffer = this.CreateMessage();
        var reader = new SequenceReader<byte>(new ReadOnlySequence<byte>(buffer.ToArray()));
        Assert.That(ReceiveRef.CreateTopicMessage.TryRead(reader, out var message, out var size), Is.True);
        Assert.That(size, Is.EqualTo(buffer.Length));
        Assert.That(message.TopicName.Value == "topic");
        Assert.That(message.PartitionCount.Value, Is.EqualTo(3));
    }

}