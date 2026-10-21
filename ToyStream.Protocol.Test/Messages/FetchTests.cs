namespace ToyStream.Protocol.Test.Messages;

using System.Buffers;
using System.Buffers.Binary;
using System.IO.Pipelines;
using System.Text;
using ToyStream.Protocol.Messages;
using ToyStream.Protocol.Responses;
using ToyStream.Shared;

using static ProtocolTestHelpers;

/// <summary>
/// A fetch names its topics once and references them by index, so these pin that the table and the
/// entries survive the trip, and that a lying count or index cannot be acted on.
/// </summary>
public class FetchTests
{
    private static readonly Utf8String[] Topics =
        [new Utf8String("events"), new Utf8String("audit"), new Utf8String("metrics")];

    private static FetchEntry[] Entries(int count)
    {
        var entries = new FetchEntry[count];
        for (var i = 0; i < count; i++)
        {
            entries[i] = new FetchEntry((short)(i % Topics.Length), (short)i, i * 1000L);
        }

        return entries;
    }

    private static byte[] WriteRequest(Utf8String[] topics, FetchEntry[] entries, int maxWait = 500,
        int maxBytes = 65536)
    {
        GetWriteBuffer(out var buffer, out var writer, size: (entries.Length * 16) + 1024);
        WriteMessage(
            new FetchRequestMessage
            {
                MaxWaitMilliseconds = maxWait,
                MaxBytes = maxBytes,
                Topics = new TopicNames(topics),
                Entries = new FetchEntries(entries)
            }, writer);

        return buffer.WrittenSpan.ToArray();
    }

    [TestCase(0)]
    [TestCase(1)]
    [TestCase(ProtocolConstants.MaxFetchEntries)]
    public void RequestRoundTrips(int entryCount)
    {
        var entries = Entries(entryCount);
        var reader = new PrimitiveReader(new ReadOnlySequence<byte>(WriteRequest(Topics, entries)));

        Assert.That(TryReadMessage<FetchRequestMessage>(ref reader, out var received, out _), Is.True);

        Assert.That(received.MaxWaitMilliseconds, Is.EqualTo(500));
        Assert.That(received.MaxBytes, Is.EqualTo(65536));
        Assert.That(received.Topics.Count, Is.EqualTo(Topics.Length));
        for (var i = 0; i < Topics.Length; i++)
        {
            Assert.That((string)received.Topics[i], Is.EqualTo((string)Topics[i]), $"topic {i} differed");
        }

        Assert.That(received.Entries.Count, Is.EqualTo(entryCount));
        var index = 0;
        foreach (var entry in received.Entries)
        {
            Assert.That(entry, Is.EqualTo(entries[index]), $"entry {index} differed");
            index++;
        }
    }

    /// <summary>The table and the entries are views over the buffer, so a split must not disturb them.</summary>
    [Test]
    public void RequestDecodesAcrossSegmentBoundaries()
    {
        var entries = Entries(5);
        var encoded = WriteRequest(Topics, entries);

        for (var split = 1; split < encoded.Length; split++)
        {
            var reader = new PrimitiveReader(Segmented(encoded, split));
            Assert.That(TryReadMessage<FetchRequestMessage>(ref reader, out var received, out _), Is.True,
                $"split at {split} failed to parse");

            Assert.That((string)received.Topics[2], Is.EqualTo((string)Topics[2]), $"split at {split} lost a topic");
            Assert.That(received.Entries[4], Is.EqualTo(entries[4]), $"split at {split} lost an entry");
        }
    }

    [Test]
    public void ResponseRoundTripsItsTopicTable()
    {
        GetWriteBuffer(out var buffer, out var writer, size: 1024);
        WriteMessage(
            new FetchResponseMessage
            {
                Result = FetchResult.Success,
                Topics = new TopicNames(Topics)
            }, writer);

        var reader = new PrimitiveReader(new ReadOnlySequence<byte>(buffer.WrittenSpan.ToArray()));
        Assert.That(TryReadMessage<FetchResponseMessage>(ref reader, out var received, out _), Is.True);

        Assert.That(received.Result, Is.EqualTo(FetchResult.Success));
        Assert.That(received.Topics.Count, Is.EqualTo(Topics.Length));
        for (var i = 0; i < Topics.Length; i++)
        {
            Assert.That((string)received.Topics[i], Is.EqualTo((string)Topics[i]), $"topic {i} differed");
        }
    }

    /// <summary>
    /// Records are streamed rather than parsed out of a buffer, so nothing bounds one but a chunk
    /// header: a body far larger than the pipe's own threshold has to survive the trip.
    /// </summary>
    [TestCase(0)]
    [TestCase(10)]
    [TestCase(300 * 1024)]
    public async Task RecordStreamRoundTrips(int bodySize)
    {
        var records = new[]
        {
            Record(0, 0, 0, "k0", 16),
            Record(1, 2, 7, "", 0),
            Record(2, 1, 99, "key-two", bodySize)
        };

        var pipe = new Pipe();
        var writing = WriteRecordStream(pipe.Writer, records);

        await using var response = await ReadResponse(pipe.Reader, Topics);
        Assert.That(response.Result, Is.EqualTo(FetchResult.Success));

        var index = 0;
        while (await response.MoveNextAsync())
        {
            var expected = records[index];
            Assert.That(response.Current.TopicIndex, Is.EqualTo(expected.TopicIndex));
            Assert.That(response.Current.PartitionId, Is.EqualTo(expected.PartitionId));
            Assert.That(response.Current.Offset, Is.EqualTo(expected.Offset));
            Assert.That(response.Current.Timestamp, Is.EqualTo(expected.Timestamp));
            Assert.That(response.Key.ToArray(), Is.EqualTo(expected.Key.ToArray()), $"record {index} key differed");

            var body = new TestPipeWriter();
            await response.CopyBodyTo(body);
            Assert.That(body.WrittenSpan.ToArray(), Is.EqualTo(expected.Body.ToArray()),
                $"record {index} body differed");
            index++;
        }

        Assert.That(index, Is.EqualTo(records.Length));
        await writing;
    }

    /// <summary>A reader that skips a body must land on the next record, not inside one.</summary>
    [Test]
    public async Task SkippedBodiesDoNotDesynchroniseTheStream()
    {
        var records = new[] { Record(0, 0, 0, "a", 70_000), Record(1, 1, 1, "b", 8) };

        var pipe = new Pipe();
        var writing = WriteRecordStream(pipe.Writer, records);

        await using var response = await ReadResponse(pipe.Reader, Topics);

        Assert.That(await response.MoveNextAsync(), Is.True);
        Assert.That(await response.MoveNextAsync(), Is.True, "the skipped body swallowed the next record");
        Assert.That(response.Current.Offset, Is.EqualTo(1));
        Assert.That(response.Key.ToArray(), Is.EqualTo("b"u8.ToArray()));
        Assert.That(await response.MoveNextAsync(), Is.False);

        await writing;
    }

    /// <summary>A failure carries no records, so the stream ends where the header did.</summary>
    [Test]
    public async Task FailureStreamsNoRecords()
    {
        var pipe = new Pipe();
        var writing = WriteRecordStream(pipe.Writer, []);

        await using var response = await ReadResponse(pipe.Reader, [], FetchResult.UnknownTopic);

        Assert.That(response.Result, Is.EqualTo(FetchResult.UnknownTopic));
        Assert.That(response.Topics.Length, Is.EqualTo(0));
        Assert.That(await response.MoveNextAsync(), Is.False);

        await writing;
    }

    private static async Task WriteRecordStream(PipeWriter pipe, FetchedRecord[] records)
    {
        var body = new MessageBodyWriter(pipe);
        try
        {
            var writer = new FetchedRecordWriter(body);
            foreach (var record in records)
            {
                await writer.WriteRecord(record, default);
            }

            await writer.CompleteAsync(default);
        }
        finally
        {
            body.Complete();
        }
    }

    /// <summary>
    /// Stands in for the connection: parse the header, then hand what is left of the buffer to the
    /// body reader the response reads through.
    /// </summary>
    private static async Task<FetchResponse> ReadResponse(PipeReader pipe, Utf8String[] topics,
        FetchResult result = FetchResult.Success)
    {
        var bodyReader = new MessageBodyReader(pipe);
        var first = await pipe.ReadAsync();
        return Create(bodyReader, first.Buffer, topics, result);

        static FetchResponse Create(MessageBodyReader bodyReader, ReadOnlySequence<byte> buffer,
            Utf8String[] topics, FetchResult result)
        {
            bodyReader.Begin(declaredSize: null, buffer);
            var header = new FetchResponseMessage { Result = result, Topics = new TopicNames(topics) };
            return CreateStreamedResponse<FetchResponseMessage, FetchResponse>(in header, bodyReader);
        }
    }

    /// <summary>
    /// A failure carries no table, so the reader must not expect one: consuming bytes that were
    /// never written would desynchronise everything after it.
    /// </summary>
    [Test]
    public void FailureRoundTripsWithoutConsumingTheFollowingMessage()
    {
        GetWriteBuffer(out var buffer, out var writer);
        WriteMessage(
            new FetchResponseMessage
            {
                Result = FetchResult.UnknownTopic,
                Topics = default
            }, writer);

        writer.Write(long.MaxValue);

        var reader = new PrimitiveReader(new ReadOnlySequence<byte>(buffer.WrittenSpan.ToArray()));
        Assert.That(TryReadMessage<FetchResponseMessage>(ref reader, out var received, out _), Is.True);

        Assert.That(received.Result, Is.EqualTo(FetchResult.UnknownTopic));
        Assert.That(reader.TryReadInt64(out var following, out _), Is.True, "the following message was consumed");
        Assert.That(following, Is.EqualTo(long.MaxValue));
    }

    [Test]
    public void IncompleteRequestReportsWhatIsMissing()
    {
        var encoded = WriteRequest(Topics, Entries(3));
        var truncated = encoded[..^1];
        var reader = new PrimitiveReader(new ReadOnlySequence<byte>(truncated));

        Assert.That(TryReadMessage<FetchRequestMessage>(ref reader, out _, out var bytesToRead), Is.False);
        Assert.That(bytesToRead, Is.EqualTo(1));
    }

    [TestCase((short)-1)]
    [TestCase(short.MaxValue)]
    public void OutOfRangeTopicCountIsRejected(short count)
    {
        var encoded = new byte[(sizeof(int) * 2) + sizeof(short)];
        BinaryPrimitives.WriteInt16BigEndian(encoded.AsSpan(sizeof(int) * 2), count);
        var reader = new PrimitiveReader(new ReadOnlySequence<byte>(encoded));

        try
        {
            TryReadMessage<FetchRequestMessage>(ref reader, out _, out _);
            Assert.Fail($"a topic count of {count} was accepted");
        }
        catch (InvalidOperationException)
        {
        }
    }

    [TestCase((short)-1)]
    [TestCase(short.MaxValue)]
    public void OutOfRangeEntryCountIsRejected(short count)
    {
        GetWriteBuffer(out var buffer, out var writer);
        writer.Write(0);
        writer.Write(0);
        new TopicNames(Topics).Write(writer);
        writer.Write(count);

        var reader = new PrimitiveReader(new ReadOnlySequence<byte>(buffer.WrittenSpan.ToArray()));

        try
        {
            TryReadMessage<FetchRequestMessage>(ref reader, out _, out _);
            Assert.Fail($"an entry count of {count} was accepted");
        }
        catch (InvalidOperationException)
        {
        }
    }

    private static FetchedRecord Record(short topicIndex, short partitionId, long offset, string key, int bodySize)
    {
        var body = new byte[bodySize];
        Random.Shared.NextBytes(body);

        return new FetchedRecord(topicIndex, partitionId, offset,
            new DateTime(2026, 10, 20, 1, 2, 3, DateTimeKind.Utc),
            new ReadOnlySequence<byte>(Encoding.UTF8.GetBytes(key)),
            new ReadOnlySequence<byte>(body));
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
