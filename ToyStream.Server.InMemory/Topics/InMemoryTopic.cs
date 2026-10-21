using System.Diagnostics.CodeAnalysis;
using ToyStream.Shared;

namespace ToyStream.Server.InMemory;

public sealed class InMemoryTopic: ITopic
{
    private readonly InMemoryPartition[] partitions;

    public InMemoryTopic(Utf8String name, short partitionCount)
    {
        Name = name;
        this.partitions = new InMemoryPartition[partitionCount];
        for (var i = 0; i < partitionCount; i++)
        {
            this.partitions[i] = new InMemoryPartition();
        }
    }

    public Utf8String Name { get; }

    public int PartitionCount => this.partitions.Length;

    public InMemoryPartition? GetPartition(short partitionId) =>
        (uint)partitionId < (uint)this.partitions.Length ? this.partitions[partitionId] : null;

    public bool TryGetPartition(short partitionId, [NotNullWhen(true)] out ITopicPartition? topicPartition)
    {
        // Unsigned so a negative id, which the sender chooses, is rejected by the same comparison.
        if ((uint)partitionId >= (uint)this.partitions.Length)
        {
            topicPartition = null;
            return false;
        }

        topicPartition = partitions[partitionId];
        return true;
    }

    public void Describe(Span<PartitionDescription> destination)
    {
        for (var i = 0; i < this.partitions.Length; i++)
        {
            destination[i] = this.partitions[i].Describe();
        }
    }
}
