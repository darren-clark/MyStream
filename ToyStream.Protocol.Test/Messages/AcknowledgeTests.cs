namespace ToyStream.Protocol.Test.Messages;

using System.Buffers;
using Protocol.Messages;

using static ProtocolTestHelpers;

public class AcknowledgeTests
{
    private static void GetWriteBuffer(out TestPipeWriter buffer, out PrimitiveWriter writer, int size = 1024)
    {
        buffer = new TestPipeWriter();
        writer = new PrimitiveWriter(buffer);
    }
    
    private static void GetReadBuffer(out ReadOnlySequence<byte> buffer, out PrimitiveReader reader)
    {
        buffer = new ReadOnlySequence<byte>(new byte[1024]);
        reader = new (buffer);
    }
    
    [Test]
    public void TestSend()
    {

        GetWriteBuffer(out var buffer, out var writer);
        WriteMessage(new AcknowledgeMessage(), writer);
        Assert.That(buffer.WrittenCount, Is.EqualTo(0));
    }

    [Test]
    public void TestReceive()
    {
        GetReadBuffer(out var buffer, out var reader);
        Assert.That(TryReadMessage<AcknowledgeMessage>(ref reader, out var message, out var bytesToRead), Is.True);
        Assert.That(bytesToRead, Is.EqualTo(0));
        Assert.That(reader.Consumed, Is.EqualTo(0));
    }
}