using System.Diagnostics.CodeAnalysis;

using MyStream.Protocol;
using MyStream.Protocol.Primitives;

namespace MyStream.Server.Topics;

public interface ITopic
{
    /// <summary>Owned by the topic, so a parked request can name it without the buffer it arrived in.</summary>
    Utf8String Name { get; }

    /// <summary>Bounded at creation, so a caller may size stack memory from it.</summary>
    int PartitionCount { get; }

    bool TryGetPartition(short partitionId, [NotNullWhen(true)] out ITopicPartition? topicPartition);

    void Describe(Span<PartitionDescription> destination);
}