using ToyStream.Shared;

namespace ToyStream.Server;

/// <summary>
/// Where a group's progress lives. Keyed by partition rather than by connection, so a checkpoint
/// outlives the subscriber that wrote it and whoever picks the partition up next finds it.
/// </summary>
public interface IConsumerGroupService
{
    /// <summary>False when the group has never committed this partition, which is not an error.</summary>
    bool TryGetCommittedOffset(Utf8String group, Utf8String topic, short partition, out long offset);

    void Commit(Utf8String group, Utf8String topic, short partition, long offset);
}