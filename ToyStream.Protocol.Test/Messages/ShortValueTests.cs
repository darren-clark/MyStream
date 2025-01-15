namespace ToyStream.Protocol.Test.Messages;

using System.Buffers;
using System.Buffers.Binary;
using Shared;

public class ShortValueTests
{

    [Test]
    public void TestReceive_NotEnoughSize()
    {
        var reader = new SequenceReader<byte>(new ReadOnlySequence<byte>(new byte[1]));
        Assert.That(Receive.ShortValue.TryRead(ref reader, out var component, out var bytesToRead), Is.False);
        Assert.That(component, Is.Null);
        Assert.That(bytesToRead, Is.EqualTo(1));
    }

    [Test]
    public void TestReceive_Enough()
    {
        //Allocate more than we need, we can make sure we don't read more than we need
        var buffer = new byte[4];
        BinaryPrimitives.WriteInt16BigEndian(buffer, 12);
        var reader = new SequenceReader<byte>(new ReadOnlySequence<byte>(buffer));
        Assert.That(Receive.ShortValue.TryRead(ref reader, out var component, out var bytesToRead), Is.True);
        Assert.That(component, Is.Not.Null);
        Assert.That(component.Value.Value, Is.EqualTo(12));
        Assert.That(bytesToRead, Is.EqualTo(0));
        Assert.That(reader.Consumed, Is.EqualTo(2));
    }

    [Test]
    public void TestWrite()
    {
        var value = new Send.ShortValue(12);
        var buffer = new byte [value.Size];
        var writer = new BigEndianSpanWriter(buffer);
        value.Write(ref writer);
        
        Assert.That(BinaryPrimitives.ReadInt16BigEndian(buffer), Is.EqualTo(12));
        Assert.That(writer.Written.Length, Is.EqualTo(2));
    }
}