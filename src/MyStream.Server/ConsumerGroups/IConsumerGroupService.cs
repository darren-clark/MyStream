using MyStream.Protocol.Primitives;

namespace MyStream.Server.ConsumerGroups;

public interface IConsumerGroupService
{

    /// <summary>False when the group has never committed this partition, which is not an error.</summary>
    bool TryGetCommittedOffset(RefUtf8String group, RefUtf8String topic, short partition, out long offset);

    void Commit(RefUtf8String group, RefUtf8String topic, short partition, long offset);
}
 