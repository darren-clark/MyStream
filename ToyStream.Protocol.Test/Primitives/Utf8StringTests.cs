namespace ToyStream.Protocol.Test.Primitives;

using System.Buffers.Binary;
using System.Text;
using Shared;

using static Messages.ProtocolTestHelpers;

public class Utf8StringTests
{
    [Test]
    public void TestSend()
    {
        var value = new Utf8String("Hello");
        var encoded = Encoding.UTF8.GetBytes("Hello");
        
        GetWriteBuffer(out var buffer, out var writer);
        writer.Write(value);
        Assert.That(buffer.WrittenCount, Is.EqualTo(sizeof(short) + encoded.Length));
        Assert.That(BinaryPrimitives.ReadInt16BigEndian(buffer.WrittenSpan), Is.EqualTo(encoded.Length));
        Assert.That(Encoding.UTF8.GetString(buffer.WrittenSpan.Slice(2)), Is.EqualTo("Hello"));
    }
    
    [Test]
    public void TestReceive_NotEnoughForSize()
    {
        GetReadBuffer(out var buffer, out var reader, size: 1);
        Assert.That( reader.TryReadUtf8String(out var component, out var bytesToRead), Is.False);
        Assert.That(bytesToRead, Is.EqualTo(1));    
    }

    [Test]
    public void TestReceive_OnlySize()
    {
        GetReadBuffer(out var buffer, out var reader, size: 2);
        BinaryPrimitives.WriteInt16BigEndian(buffer, 6);
        Assert.That(reader.TryReadUtf8String(out var component, out var bytesToRead), Is.False);
        Assert.That(bytesToRead, Is.EqualTo(6));
    }
    
    [Test]
    public void TestReceive_SizeAndPartial()
    {
        GetReadBuffer(out var buffer, out var reader, size: 4);
        // Buffer contains size and first 2 bytes of the string
        BinaryPrimitives.WriteInt16BigEndian(buffer, 6);
        Assert.That(reader.TryReadUtf8String(out var component, out var bytesToRead), Is.False);
        Assert.That(bytesToRead, Is.EqualTo(4));
    }

    /// <summary>
    /// The length is taken on trust from the wire, and the reader slices by it, so a negative one
    /// must be rejected rather than reaching SequenceReader.
    /// </summary>
    [TestCase((short)-1)]
    [TestCase(short.MinValue)]
    public void TestReceive_NegativeLengthIsRejected(short length)
    {
        GetReadBuffer(out var buffer, out var reader);
        BinaryPrimitives.WriteInt16BigEndian(buffer, length);

        try
        {
            reader.TryReadUtf8String(out _, out _);
            Assert.Fail("a negative length was accepted");
        }
        catch (InvalidOperationException)
        {
        }
    }

    [Test]
    public void TestReceive_Complete()
    {
        var encoded = "Hello"u8;
        GetReadBuffer(out var buffer, out var reader);
        // Buffer contains size and string and an extra two bytes of the next message
        BinaryPrimitives.WriteInt16BigEndian(buffer, (short) encoded.Length);
        encoded.CopyTo(buffer.Slice(2));
        Assert.That(reader.TryReadUtf8String(out var component, out var bytesToRead), Is.True);
        Assert.That(bytesToRead, Is.EqualTo(0));
        Assert.That(reader.Consumed, Is.EqualTo(2+encoded.Length));
        Assert.That((string) component, Is.EqualTo("Hello"));
    }
}