namespace ToyStream.Protocol.Test.Messages;

using System.Buffers;
using System.Buffers.Binary;
using System.Text;
using Shared;

public class Utf8StringTests
{
    [Test]
    public void TestSend()
    {
        var value = new Send.Utf8String("Hello");
        var encoded = Encoding.UTF8.GetBytes("Hello");
        
        Assert.That(value.Size, Is.EqualTo(2+encoded.Length));
        
        Span<byte> buffer = new byte[value.Size];
        var writer = new SpanWriter(buffer);
        value.Write(ref writer);
        Assert.That(writer.Written.Length, Is.EqualTo(value.Size));
        Assert.That(BinaryPrimitives.ReadInt16BigEndian(writer.Written), Is.EqualTo(encoded.Length));
        Assert.That(Encoding.UTF8.GetString(writer.Written.Slice(2)), Is.EqualTo("Hello"));
    }
    
    [Test]
    public void TestReceive_NotEnoughForSize()
    {
        var reader = new SequenceReader<byte>(new ReadOnlySequence<byte>(new byte[1]));
        Assert.That(Receive.Utf8String.TryRead(ref reader, out var component, out var bytesToRead), Is.False);
        Assert.That(component, Is.Null);
        Assert.That(bytesToRead, Is.EqualTo(1));    
    }

    [Test]
    public void TestReceive_OnlySize()
    {
        var buffer = new byte[2];
        BinaryPrimitives.WriteInt16BigEndian(buffer, 6);
        var reader = new SequenceReader<byte>(new ReadOnlySequence<byte>(buffer));
        Assert.That(Receive.Utf8String.TryRead(ref reader, out var component, out var bytesToRead), Is.False);
        Assert.That(component, Is.Null);
        Assert.That(bytesToRead, Is.EqualTo(6));
    }
    
    [Test]
    public void TestReceive_SizeAndPartial()
    {
        // Buffer contains size and first 2 bytes of the string
        var buffer = new byte[4];
        BinaryPrimitives.WriteInt16BigEndian(buffer, 6);
        var reader = new SequenceReader<byte>(new ReadOnlySequence<byte>(buffer));
        Assert.That(Receive.Utf8String.TryRead(ref reader, out var component, out var bytesToRead), Is.False);
        Assert.That(component, Is.Null);
        Assert.That(bytesToRead, Is.EqualTo(4));
    }

    [Test]
    public void TestReceive_Complete()
    {
        var value = "Hello"u8;
        // Buffer contains size and string and an extra two bytes of the next message
        var buffer = new byte[2+value.Length+2];
        BinaryPrimitives.WriteInt16BigEndian(buffer, (short) value.Length);
        value.CopyTo(buffer.AsSpan(2));
        var reader = new SequenceReader<byte>(new ReadOnlySequence<byte>(buffer));
        Assert.That(Receive.Utf8String.TryRead(ref reader, out var component, out var bytesToRead), Is.True);
        Assert.That(component, Is.Not.Null);
        Assert.That(bytesToRead, Is.EqualTo(0));
        Assert.That(reader.Consumed, Is.EqualTo(2+value.Length));
        Assert.That(component!.Value.Value, Is.EqualTo("Hello"));
    }
    
        [Test]
    public void TestReceiveRef_NotEnoughForSize()
    {
        var reader = new SequenceReader<byte>(new ReadOnlySequence<byte>(new byte[1]));
        Assert.That(ReceiveRef.Utf8String.TryRead(ref reader, out var component, out var bytesToRead), Is.False);
        Assert.That(component, Is.Null);
        Assert.That(bytesToRead, Is.EqualTo(1));    
    }

    [Test]
    public void TestReceiveRef_OnlySize()
    {
        var buffer = new byte[2];
        BinaryPrimitives.WriteInt16BigEndian(buffer, 6);
        var reader = new SequenceReader<byte>(new ReadOnlySequence<byte>(buffer));
        Assert.That(ReceiveRef.Utf8String.TryRead(ref reader, out var component, out var bytesToRead), Is.False);
        Assert.That(component, Is.Null);
        Assert.That(bytesToRead, Is.EqualTo(6));
    }
    
    [Test]
    public void TestReceiveRef_SizeAndPartial()
    {
        // Buffer contains size and first 2 bytes of the string
        var buffer = new byte[4];
        BinaryPrimitives.WriteInt16BigEndian(buffer, 6);
        var reader = new SequenceReader<byte>(new ReadOnlySequence<byte>(buffer));
        Assert.That(ReceiveRef.Utf8String.TryRead(ref reader, out var component, out var bytesToRead), Is.False);
        Assert.That(component, Is.Null);
        Assert.That(bytesToRead, Is.EqualTo(4));
    }

    [Test]
    public void TestReceiveRef_Complete()
    {
        var value = "Hello"u8;
        // Buffer contains size and string and an extra two bytes of the next message
        var buffer = new byte[2+value.Length+2];
        BinaryPrimitives.WriteInt16BigEndian(buffer, (short) value.Length);
        value.CopyTo(buffer.AsSpan(2));
        var reader = new SequenceReader<byte>(new ReadOnlySequence<byte>(buffer));
        Assert.That(ReceiveRef.Utf8String.TryRead(ref reader, out var component, out var bytesToRead), Is.True);
        Assert.That(component, Is.Not.Null);
        Assert.That(bytesToRead, Is.EqualTo(0));
        Assert.That(reader.Consumed, Is.EqualTo(2+value.Length));
        Assert.That(component!.Value.Value, Is.EqualTo("Hello"));
    }
}