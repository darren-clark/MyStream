namespace ToyStream.Protocol.Test.Messages;

using System.Buffers;
using System.Buffers.Binary;
using ToyStream.Protocol.Messages;
using ToyStream.Shared;
using ToyStream.Protocol.Responses;

using static ProtocolTestHelpers;

/// <summary>
/// The partition list is a view over the buffer rather than an array, so these pin that it decodes
/// to the same records however the pipe split it, and that a lying count cannot be acted on.
/// </summary>
public class DescribeTopicTests
{
    private static PartitionDescription[] Descriptions(int count)
    {
        var descriptions = new PartitionDescription[count];
        for (var i = 0; i < count; i++)
        {
            descriptions[i] = new PartitionDescription(i * 10L, (i * 10L) + i);
        }

        return descriptions;
    }

    [TestCase(0)]
    [TestCase(1)]
    [TestCase(ProtocolConstants.MaxPartitions)]
    public void PartitionsRoundTrip(int count)
    {
        var sent = Descriptions(count);

        GetWriteBuffer(out var buffer, out var writer, size: (count * 16) + 64);
        WriteMessage(new DescribeTopicResponseMessage { Result = DescribeTopicResult.Success, Partitions = new PartitionDescriptions(sent) }, writer);

        var reader = new PrimitiveReader(new ReadOnlySequence<byte>(buffer.WrittenSpan.ToArray()));
        Assert.That(TryReadMessage<DescribeTopicResponseMessage>(ref reader, out var received, out _), Is.True);

        Assert.That(received.Partitions.Count, Is.EqualTo(count));

        var index = 0;
        foreach (var description in received.Partitions)
        {
            Assert.That(description, Is.EqualTo(sent[index]), $"partition {index} differed");
            index++;
        }

        Assert.That(index, Is.EqualTo(count));
    }

    /// <summary>A split run must decode identically: the records are read, not sliced as a span.</summary>
    [Test]
    public void PartitionsDecodeAcrossSegmentBoundaries()
    {
        var sent = Descriptions(5);

        GetWriteBuffer(out var buffer, out var writer);
        WriteMessage(new DescribeTopicResponseMessage { Result = DescribeTopicResult.Success, Partitions = new PartitionDescriptions(sent) }, writer);
        var encoded = buffer.WrittenSpan.ToArray();

        // Split mid record, so a decode that assumed contiguity would read the wrong bytes.
        var reader = new PrimitiveReader(Segmented(encoded, splitAt: 2 + 4));
        Assert.That(TryReadMessage<DescribeTopicResponseMessage>(ref reader, out var received, out _), Is.True);

        for (var i = 0; i < sent.Length; i++)
        {
            Assert.That(received.Partitions[i], Is.EqualTo(sent[i]), $"partition {i} differed");
        }
    }

    [Test]
    public void IncompleteRecordsReportWhatIsMissing()
    {
        var sent = Descriptions(3);

        GetWriteBuffer(out var buffer, out var writer);
        WriteMessage(new DescribeTopicResponseMessage { Result = DescribeTopicResult.Success, Partitions = new PartitionDescriptions(sent) }, writer);

        // One byte short of the final record.
        var truncated = buffer.WrittenSpan[..(buffer.WrittenCount - 1)].ToArray();
        var reader = new PrimitiveReader(new ReadOnlySequence<byte>(truncated));

        Assert.That(TryReadMessage<DescribeTopicResponseMessage>(ref reader, out _, out var bytesToRead), Is.False);
        Assert.That(bytesToRead, Is.EqualTo(1));
    }

    /// <summary>
    /// A failure carries no partitions, so the reader must not expect a count either: consuming two
    /// bytes that were never written would desynchronise everything after it.
    /// </summary>
    [Test]
    public void FailureRoundTripsWithoutConsumingTheFollowingMessage()
    {
        GetWriteBuffer(out var buffer, out var writer);
        WriteMessage(
            new DescribeTopicResponseMessage
            {
                Result = DescribeTopicResult.UnknownTopic,
                Partitions = default
            }, writer);

        // A recognisable message after it, which a mis-framed read would eat into.
        writer.Write(long.MaxValue);

        var reader = new PrimitiveReader(new ReadOnlySequence<byte>(buffer.WrittenSpan.ToArray()));
        Assert.That(TryReadMessage<DescribeTopicResponseMessage>(ref reader, out var received, out _), Is.True);

        Assert.That(received.Result, Is.EqualTo(DescribeTopicResult.UnknownTopic));
        Assert.That(received.Partitions.Count, Is.EqualTo(0));

        Assert.That(reader.TryReadInt64(out var following, out _), Is.True, "the following message was consumed");
        Assert.That(following, Is.EqualTo(long.MaxValue));
    }

    /// <summary>The count decides how much is read, so it is bounded before it is acted on.</summary>
    [TestCase((short)-1)]
    [TestCase(short.MaxValue)]
    public void OutOfRangePartitionCountIsRejected(short count)
    {
        var encoded = new byte[sizeof(byte) + sizeof(short)];
        encoded[0] = (byte)DescribeTopicResult.Success;
        BinaryPrimitives.WriteInt16BigEndian(encoded.AsSpan(sizeof(byte)), count);
        var reader = new PrimitiveReader(new ReadOnlySequence<byte>(encoded));

        try
        {
            TryReadMessage<DescribeTopicResponseMessage>(ref reader, out _, out _);
            Assert.Fail($"a partition count of {count} was accepted");
        }
        catch (InvalidOperationException)
        {
        }
    }

    private static ReadOnlySequence<byte> Segmented(byte[] value, int splitAt)
    {
        var first = new Segment(value[..splitAt]);
        var last = first.Append(value[splitAt..]);
        return new ReadOnlySequence<byte>(first, 0, last, last.Memory.Length);
    }

    private sealed class Segment : ReadOnlySequenceSegment<byte>
    {
        public Segment(ReadOnlyMemory<byte> memory) => Memory = memory;

        public Segment Append(ReadOnlyMemory<byte> memory)
        {
            var next = new Segment(memory) { RunningIndex = RunningIndex + Memory.Length };
            Next = next;
            return next;
        }
    }
}
