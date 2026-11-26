using System.Diagnostics.CodeAnalysis;

using MyStream.Protocol.Primitives;
using MyStream.Server.Topics;

namespace MyStream.Server.InMemory.Topics;

internal class InMemoryTopic: ITopic
{
    private long version = 0;
    
    private readonly Dictionary<short, ITopicPartition> partitions = new();
    public Utf8String Name { get; }
    public int PartitionCount { get; }
    
    public bool TryGetPartition(short partitionId, [NotNullWhen(true)] out ITopicPartition? topicPartition)
    {
        throw new NotImplementedException();
    }

    public bool TryGetPartitionReference(short partitionId, [NotNullWhen(true)] out PartitionReference? reference)
    {
        if (partitions.TryGetValue(partitionId, out ITopicPartition? topicPartition))
        {
            
        }
        throw new NotImplementedException();
    }

    public ITopicPartition GetPartitionByIndex(short index)
    {
        throw new NotImplementedException();
    }

    public void Describe(Span<PartitionDescription> destination)
    {
        throw new NotImplementedException();
    }

    public IReadOnlyCollection<ITopicPartition> GetPartitions()
    {
        throw new NotImplementedException();
    }
}