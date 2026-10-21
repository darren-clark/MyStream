using System.Buffers;

namespace ToyStream.Server.InMemory;

using System.Diagnostics.CodeAnalysis;

/// <summary>
/// A stored event. The body borrows whatever the writer filled - pooled blocks, in the case of a
/// recyclable stream - so the owner is held alongside it and released when the event is dropped.
/// </summary>
internal sealed record Event(DateTime receivedTime, Memory<byte> Key, ReadOnlySequence<byte> Body, IDisposable? BodyOwner = null)
    : IDisposable
{
    public void Dispose() => BodyOwner?.Dispose();
}

/// <summary>
/// An append-only log addressed by dense sequential offsets. Segmented rather than one growing
/// array so that growth never moves what is already stored, which is what lets readers walk it
/// while writers append.
/// </summary>
internal sealed class EventLog
{
    private const int SegmentShift = 10;
    private const int SegmentSize = 1 << SegmentShift;
    private const int SegmentMask = SegmentSize - 1;

    private readonly Lock appendLock = new();

    private Event[]?[] segments = new Event[4][];
    private long count;

    /// <summary>Committed events: every offset below this is readable.</summary>
    public long Count => Volatile.Read(ref this.count);

    /// <summary>
    /// Offsets are assigned under the lock rather than by an interlocked counter, so a reader can
    /// never reach a reserved but unfilled slot. The lock covers an array store, not the body copy.
    /// </summary>
    public long Append(Event @event)
    {
        lock (this.appendLock)
        {
            var offset = this.count;
            var current = EnsureSegment((int)(offset >> SegmentShift));
            current[(int)(offset >> SegmentShift)]![offset & SegmentMask] = @event;

            // Published last, so a reader that sees the count is guaranteed to see the slot.
            Volatile.Write(ref this.count, offset + 1);
            return offset;
        }
    }

    public bool TryGet(long offset, [NotNullWhen(true)] out Event? @event)
    {
        // Unsigned so a negative offset is rejected by the same comparison as one past the end.
        if ((ulong)offset >= (ulong)Volatile.Read(ref this.count))
        {
            @event = null;
            return false;
        }

        @event = Volatile.Read(ref this.segments)[(int)(offset >> SegmentShift)]![offset & SegmentMask];
        return true;
    }

    private Event[]?[] EnsureSegment(int segmentIndex)
    {
        var current = this.segments;
        if (segmentIndex >= current.Length)
        {
            // Only the segment pointers move; the segments themselves stay put for readers.
            var grown = new Event[current.Length * 2][];
            Array.Copy(current, grown, current.Length);
            Volatile.Write(ref this.segments, grown);
            current = grown;
        }

        current[segmentIndex] ??= new Event[SegmentSize];
        return current;
    }
}
