namespace ToyStream.Protocol.Dispatch;

using System.Buffers;
using Messages;

/// <summary>
/// Heap equivalent of <see cref="CreateTopicMessage"/>: the payload is copied out of the pipe
/// buffer so it can outlive it.
/// Holds bytes rather than a string on purpose - transcoding to UTF-16 would be a second cost on
/// top of the copy, so this is the cheapest a heap representation can be.
/// </summary>
public sealed class HeapCreateTopicMessage
{
    public HeapCreateTopicMessage(ref readonly CreateTopicMessage message)
    {
        this.TopicName = message.TopicName.Sequence.ToArray();
        this.PartitionCount = message.PartitionCount;
    }

    public byte[] TopicName { get; }

    public short PartitionCount { get; }
}
