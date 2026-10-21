using System.Diagnostics.CodeAnalysis;

namespace ToyStream.Server;

public interface ITopic
{
    bool TryGetPartition(short partitionId, [NotNullWhen(true)] out ITopicPartition? topicPartition);
}