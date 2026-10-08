using ToyStream.Shared;

namespace ToyStream.Protocol.Dispatch.Context;

public interface ITopicService
{
    ValueTask CreateTopic(RefUtf8String messageTopicName, short messagePartitionCount);
}