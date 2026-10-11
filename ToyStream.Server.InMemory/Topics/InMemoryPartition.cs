namespace ToyStream.Server.InMemory;

public class InMemoryPartition: ITopicPartition
{
    private List<byte[]> events = new List<byte[]>();
    
}