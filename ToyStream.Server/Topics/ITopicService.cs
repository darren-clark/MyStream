using System.Diagnostics.CodeAnalysis;
using System.IO.Pipelines;
using ToyStream.Protocol.Responses;
using ToyStream.Shared;

namespace ToyStream.Server;


public interface ITopicService
{
    ValueTask<CreateTopicResult> CreateTopic(RefUtf8String topicName, short partitionCount);
    
    bool TryStartEventWrite(RefUtf8String topicName, short partitionId, RefBytes key, long? size, out PublishResult failure, [NotNullWhen(true)] out IEventWriter? writer);
}