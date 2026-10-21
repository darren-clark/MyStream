using ToyStream.Shared;

namespace ToyStream.Server;

public interface ITopicPartition
{
    IEventWriter GetEventWriter(RefBytes key, long? size);

    PartitionDescription Describe();

    /// <summary>
    /// One record at a time, so a response is never materialised whole. The ids are stamped in so
    /// the reader can tell where it came from without a second lookup.
    /// </summary>
    bool TryReadRecord(short topicIndex, short partitionId, long offset, out FetchedRecord record);

    /// <summary>
    /// Completes on the next append. A caught up reader waits on this rather than polling, and the
    /// caller re-reads afterwards because a wake says only that something changed.
    /// </summary>
    Task WaitForAppend(CancellationToken cancellationToken);
}