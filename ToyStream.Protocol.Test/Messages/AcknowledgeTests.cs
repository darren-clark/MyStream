namespace ToyStream.Protocol.Test.Messages;

using System.Buffers;
using Shared;

public class AcknowledgeTests
{
    [Test]
    public void TestSend()
    {
        var writer = new BigEndianSpanWriter(new byte[1024]);
        new Send.AcknowledgeMessage().Write(ref writer);
        Assert.That(writer.Written.Length, Is.EqualTo(0));
    }

    [Test]
    public void TestReceive()
    {
        var reader = new SequenceReader<byte>(new ReadOnlySequence<byte>(new byte[1024]));
        Assert.That(Receive.AcknowledgeMessage.TryRead(ref reader, out var message, out var bytesToRead), Is.True);
        Assert.That(message, Is.Not.Null);
        Assert.That(bytesToRead, Is.EqualTo(0));
    }
    
    [Test]
    public void TestReceiveRef()
    {
        var reader = new SequenceReader<byte>(new ReadOnlySequence<byte>(new byte[1024]));
        Assert.That(ReceiveRef.AcknowledgeMessage.TryRead(ref reader, out var message, out var size), Is.True);
        Assert.That(size, Is.EqualTo(0));
    }

}