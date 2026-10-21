using System.Diagnostics.CodeAnalysis;
using ToyStream.Shared;

namespace ToyStream.Server;

/// <summary>
/// Deliberately free of protocol result codes: the handler decides what a failed lookup means on
/// the wire, so a provider never has to know.
/// </summary>
public interface ITopicService
{
    /// <summary>False when the topic already exists. The count is validated before it gets here.</summary>
    ValueTask<bool> TryCreateTopic(RefUtf8String topicName, short partitionCount);

    bool TryGetTopic(RefUtf8String topicName, [NotNullWhen(true)] out ITopic? topic);
}