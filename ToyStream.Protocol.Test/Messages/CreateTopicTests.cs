namespace ToyStream.Protocol.Test.Messages;

using System.Buffers;
using Protocol.Messages;
using Shared;

using static ProtocolTestHelpers;

public class CreateTopicTests
{
    // Creates a buffer containing a serialized CreateTopicMessage with the topic "topic" and partition count 3
    private ReadOnlySpan<byte> CreateMessage()
    {
        GetWriteBuffer(out var buffer, out var writer);        
        new CreateTopicMessage
        {
            TopicName = (Utf8String)"topic",
            PartitionCount = 3
        }.Write(writer);

        return buffer.WrittenSpan;
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
        GetWriteBuffer(out var buffer, out var writer);
        writer.Write(new Utf8String("topic"));
        writer.Write((short) 3);
        
        var message = this.CreateMessage();
        Assert.That(message.ToArray(), Is.EqualTo(buffer.WrittenSpan.ToArray()) );
        this.sendPassed = true;
    }
    
    [Test]
    public void TestReceive_NotEnoughForStringSize()
    {
        // Only 1 byte, not enough for a short(string length)
        var buffer = this.CreateMessage()[..1];
        var reader = new PrimitiveReader(new ReadOnlySequence<byte>(buffer.ToArray()));
        Assert.That( ProtocolTestHelpers.TryReadMessage<CreateTopicMessage>(ref reader, out var message, out var size), Is.False);
        Assert.That(reader.Consumed, Is.EqualTo(0));
        Assert.That(size, Is.EqualTo(1));
    }

    [Test]
    public void TestReceive_NotEnoughForStringPartitionCount()
    {
        // trim one byte from the end, not enough for a short(partition count)
        var buffer = this.CreateMessage()[..^1];
        var reader = new PrimitiveReader(new ReadOnlySequence<byte>(buffer.ToArray()));
        Assert.That( ProtocolTestHelpers.TryReadMessage<CreateTopicMessage>(ref reader, out var message, out var size), Is.False);
        Assert.That(reader.Consumed, Is.EqualTo(0));
        Assert.That(size, Is.EqualTo(1));
    }

    [Test]
    public void TestReceive_Complete()
    {
        var buffer = this.CreateMessage();
        var reader = new PrimitiveReader(new ReadOnlySequence<byte>(buffer.ToArray()));
        Assert.That( ProtocolTestHelpers.TryReadMessage<CreateTopicMessage>(ref reader, out var message, out var size), Is.True);
        Assert.That(reader.Consumed, Is.EqualTo(buffer.Length));
        Assert.That(size, Is.EqualTo(0));
        Assert.That(message.TopicName.Equals(new Utf8String("topic")));
        Assert.That(message.PartitionCount, Is.EqualTo(3));
    }
}