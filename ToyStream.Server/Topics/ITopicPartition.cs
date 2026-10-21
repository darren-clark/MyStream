using ToyStream.Shared;

namespace ToyStream.Server;

public interface ITopicPartition
{
    IEventWriter GetEventWriter(RefBytes key, long? size);
}