using System.Diagnostics.CodeAnalysis;
using ToyStream.Shared;

namespace ToyStream.Server;

public interface ITopic
{
    /// <summary>Bounded at creation, so a caller may size stack memory from it.</summary>
    int PartitionCount { get; }

    bool TryGetPartition(short partitionId, [NotNullWhen(true)] out ITopicPartition? topicPartition);

    void Describe(Span<PartitionDescription> destination);
}