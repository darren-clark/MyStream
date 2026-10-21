using System.Diagnostics.CodeAnalysis;

namespace ToyStream.Server.InMemory;

public sealed class InMemoryTopic: ITopic
{
    private readonly InMemoryPartition[] partitions;

    public InMemoryTopic(short partitionCount)
    {
        this.partitions = new InMemoryPartition[partitionCount];
        for (var i = 0; i < partitionCount; i++)
        {
            this.partitions[i] = new InMemoryPartition();
        }
    }

    public int PartitionCount => this.partitions.Length;

    public InMemoryPartition? GetPartition(short partitionId) =>
        (uint)partitionId < (uint)this.partitions.Length ? this.partitions[partitionId] : null;

    public bool TryGetPartition(short partitionId, [NotNullWhen(true)] out ITopicPartition? topicPartition)
    {
        if (partitionId >= this.partitions.Length)
        {
            topicPartition = null;
            return false;
        }

        topicPartition = partitions[partitionId];
        return true;
    }
}
