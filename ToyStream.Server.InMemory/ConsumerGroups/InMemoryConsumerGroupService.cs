using System.Collections.Concurrent;
using ToyStream.Server;
using ToyStream.Shared;

namespace ToyStream.Server.InMemory;

/// <summary>
/// Checkpoints in a dictionary, which is as durable as the log they point into.
/// </summary>
public sealed class InMemoryConsumerGroupService : IConsumerGroupService
{
    private readonly ConcurrentDictionary<Checkpoint, long> committed = new();

    private readonly record struct Checkpoint(Utf8String Group, Utf8String Topic, short Partition);

    public bool TryGetCommittedOffset(Utf8String group, Utf8String topic, short partition, out long offset) =>
        this.committed.TryGetValue(new Checkpoint(group, topic, partition), out offset);

    /// <summary>
    /// Never moves backwards. A commit that lost a race to a later one would otherwise replay
    /// records the group has already accounted for, and nothing in the protocol rewinds on purpose.
    /// </summary>
    public void Commit(Utf8String group, Utf8String topic, short partition, long offset) =>
        this.committed.AddOrUpdate(new Checkpoint(group, topic, partition), offset,
            (_, existing) => Math.Max(existing, offset));
}
