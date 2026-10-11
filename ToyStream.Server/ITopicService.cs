using ToyStream.Shared;

namespace ToyStream.Server;

/// <summary>
/// The domain seam: the connection owns protocol, this owns meaning. Takes a RefUtf8String so the
/// topic name is never copied off the read buffer.
/// </summary>
public interface ITopicService
{
    ValueTask CreateTopic(RefUtf8String topicName, short partitionCount);
}