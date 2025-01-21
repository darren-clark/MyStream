namespace ToyStream.Protocol.Test.Primitives;

using System.Buffers.Binary;

using static Messages.ProtocolTestHelpers;

public class ShortTests
{
    [Test]
    public void TestReceive_NotEnoughSize()
    {
        GetReadBuffer(out var buffer, out var reader, size: 1);
        Assert.That( reader.TryReadInt16(out var component, out var bytesToRead), Is.False);
        Assert.That(bytesToRead, Is.EqualTo(1));
    }

    [Test]
    public void TestReceive_Enough()
    {
        //Allocate more than we need, we can make sure we don't read more than we need
        //var buffer = new byte[4];
        GetReadBuffer(out var buffer, out var reader);
        BinaryPrimitives.WriteInt16BigEndian(buffer, 12);
        Assert.That( reader.TryReadInt16(out var component, out var bytesToRead), Is.True);
        Assert.That(component, Is.EqualTo(12));
        Assert.That(bytesToRead, Is.EqualTo(0));
        Assert.That(reader.Consumed, Is.EqualTo(2));
    }

    [Test]
    public void TestSend()
    {
        GetWriteBuffer(out var buffer, out var writer);
        
        writer.Write((short) 12);
       
        Assert.That(BinaryPrimitives.ReadInt16BigEndian(buffer.WrittenSpan), Is.EqualTo(12));
        Assert.That(buffer.WrittenCount, Is.EqualTo(2));
    }
}