namespace ToyStream.Server.InMemory;

using System.Buffers;
using ToyStream.Protocol.Responses;

/// <summary>
/// An in-memory partition has to materialise what it stores, so it copies each chunk as it arrives
/// rather than retaining the pipe memory it is handed.
/// </summary>
public sealed class InMemoryPartition : IPartitionWriter
{
    private readonly List<byte[]> events = [];
    private readonly ArrayBufferWriter<byte> pending = new();

    public int Count
    {
        get
        {
            lock (this.events)
            {
                return this.events.Count;
            }
        }
    }

    public byte[] this[int index]
    {
        get
        {
            lock (this.events)
            {
                return this.events[index];
            }
        }
    }

    public ValueTask AppendAsync(ReadOnlySequence<byte> chunk, CancellationToken cancellationToken)
    {
        foreach (var segment in chunk)
        {
            this.pending.Write(segment.Span);
        }

        return default;
    }

    public ValueTask<PublishResult> CommitAsync(CancellationToken cancellationToken)
    {
        lock (this.events)
        {
            this.events.Add(this.pending.WrittenSpan.ToArray());
        }

        this.pending.Clear();
        return ValueTask.FromResult(PublishResult.Success);
    }
}
