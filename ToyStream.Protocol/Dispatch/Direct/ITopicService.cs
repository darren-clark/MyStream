namespace ToyStream.Protocol.Dispatch.Direct;

using Shared;

/// <summary>
/// The domain seam. Keeping it out of the connection is what preserves testability once the handler
/// abstraction is gone - the connection owns protocol, this owns meaning.
/// </summary>
public interface ITopicService
{
    ValueTask CreateTopic(RefUtf8String topicName, short partitionCount);
}
