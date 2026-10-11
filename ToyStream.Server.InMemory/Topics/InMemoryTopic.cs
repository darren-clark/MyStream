namespace ToyStream.Server.InMemory;

public class InMemoryTopic: ITopic
{
    private readonly List<InMemoryPartition> partitions;

    public InMemoryTopic(int partitionCount)
    {
        partitions = new List<InMemoryPartition>(partitionCount);
        for (var i = 0; i < partitionCount; i++)
        {
            partitions.Add(new InMemoryPartition());
        }
    }
}