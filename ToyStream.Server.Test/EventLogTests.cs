namespace ToyStream.Server.Test;

using System.Buffers;
using ToyStream.Server.InMemory;

/// <summary>
/// Offsets are dense and a reader may walk the log while writers append, so these pin both the
/// addressing and the point at which an event becomes visible.
/// </summary>
public class EventLogTests
{
    private const int SegmentSize = 1024;

    private static Event EventWithBody(int value) =>
        new(Memory<byte>.Empty, new ReadOnlySequence<byte>(BitConverter.GetBytes(value)));

    private static int BodyValue(Event @event) => BitConverter.ToInt32(@event.Body.FirstSpan);

    [Test]
    public void AppendReturnsDenseOffsetsFromZero()
    {
        var log = new EventLog();

        for (var i = 0; i < 5; i++)
        {
            Assert.That(log.Append(EventWithBody(i)), Is.EqualTo(i));
        }

        Assert.That(log.Count, Is.EqualTo(5));
    }

    /// <summary>Growth must not disturb what is already stored, including across segment boundaries.</summary>
    [Test]
    public void EventsSurviveSegmentGrowth()
    {
        var log = new EventLog();
        var total = (SegmentSize * 4) + 7;

        for (var i = 0; i < total; i++)
        {
            log.Append(EventWithBody(i));
        }

        Assert.That(log.Count, Is.EqualTo(total));

        for (var i = 0; i < total; i++)
        {
            Assert.That(log.TryGet(i, out var stored), Is.True, $"offset {i} was missing");
            Assert.That(BodyValue(stored!), Is.EqualTo(i), $"offset {i} held the wrong event");
        }
    }

    [Test]
    public void OffsetsOutsideTheLogAreRejected()
    {
        var log = new EventLog();
        log.Append(EventWithBody(0));

        Assert.That(log.TryGet(1, out _), Is.False, "an uncommitted offset was readable");
        Assert.That(log.TryGet(-1, out _), Is.False, "a negative offset was accepted");
        Assert.That(log.TryGet(long.MinValue, out _), Is.False);
    }

    /// <summary>
    /// Concurrent appends must still produce every offset exactly once, and an offset must never be
    /// visible before the slot behind it is filled.
    /// </summary>
    [Test]
    public void ConcurrentAppendsProduceEveryOffsetExactlyOnce()
    {
        var log = new EventLog();
        const int writers = 8;
        const int perWriter = 2000;

        var offsets = new System.Collections.Concurrent.ConcurrentBag<long>();
        Parallel.For(0, writers, writer =>
        {
            for (var i = 0; i < perWriter; i++)
            {
                offsets.Add(log.Append(EventWithBody((writer * perWriter) + i)));
            }
        });

        var total = writers * perWriter;
        Assert.That(log.Count, Is.EqualTo(total));
        Assert.That(offsets.Distinct().Count(), Is.EqualTo(total), "an offset was handed out twice");
        Assert.That(offsets.Min(), Is.EqualTo(0));
        Assert.That(offsets.Max(), Is.EqualTo(total - 1));

        for (long offset = 0; offset < total; offset++)
        {
            Assert.That(log.TryGet(offset, out _), Is.True, $"offset {offset} was never filled");
        }
    }

    /// <summary>
    /// A reader trailing a writer must never see a committed offset with nothing behind it. The
    /// writer yields each append so the reader stays at the frontier, which is the only place the
    /// race is observable.
    /// </summary>
    [Test]
    public void ReaderNeverSeesACommittedOffsetWithoutItsEvent()
    {
        var log = new EventLog();
        const int total = (SegmentSize * 2) + 7;

        var reader = Task.Run(() =>
        {
            long next = 0;
            while (next < total)
            {
                if (!log.TryGet(next, out var stored))
                {
                    Thread.Yield();
                    continue;
                }

                Assert.That(stored, Is.Not.Null, $"offset {next} was committed before its event was stored");
                Assert.That(BodyValue(stored!), Is.EqualTo((int)next), $"offset {next} held the wrong event");
                next++;
            }

            return next;
        });

        for (var i = 0; i < total; i++)
        {
            log.Append(EventWithBody(i));
            Thread.Yield();
        }

        Assert.That(reader.Wait(TimeSpan.FromSeconds(30)), Is.True, "the reader did not catch up");
        Assert.That(reader.Result, Is.EqualTo(total));
    }
}
