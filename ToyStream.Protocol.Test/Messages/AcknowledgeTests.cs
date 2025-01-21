namespace ToyStream.Protocol.Test.Messages;

using System.Buffers;
using Responses;

using static ProtocolTestHelpers;

public class AcknowledgeTests
{
    private static void GetWriteBuffer(out ArrayBufferWriter<byte> buffer, out PrimitiveWriter<ArrayBufferWriter<byte>> writer, int size = 1024)
    {
        buffer = new ArrayBufferWriter<byte>(1024);
        writer = new (buffer);
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
        ((IMessage<AcknowledgeMessage>)new AcknowledgeMessage()).Write(writer);
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