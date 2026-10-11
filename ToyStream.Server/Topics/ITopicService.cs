using ToyStream.Protocol.Responses;
using ToyStream.Shared;

namespace ToyStream.Server;


public interface ITopicService
{
    ValueTask<CreateTopicResult> CreateTopic(RefUtf8String topicName, short partitionCount);
}