using MyStream.Protocol.Primitives;
using MyStream.Protocol.Responses;

namespace MyStream.Protocol.Messages;

/// <summary>
/// Request to create a topic on the server.
/// </summary>
public readonly ref struct CreateTopicMessage : IRequestMessage<CreateTopicMessage, AcknowledgeMessage<CreateTopicResult>>
{
    public required RefUtf8String TopicName { get; init; }
    public required short PartitionCount { get; init; }

    public static MessageType Type => MessageType.CreateTopic;

    static bool IProtocolPrimitive<CreateTopicMessage>.TryRead<TReader>(scoped ref TReader reader, out CreateTopicMessage message,
        out int bytesToRead)
    {
        if (reader.TryReadPrimitive<RefUtf8String>(out var topicName, out bytesToRead) &&
            reader.TryReadInt16(out var partitionCount, out bytesToRead))
        {
            message = new CreateTopicMessage { TopicName = topicName, PartitionCount = partitionCount };
            return true;
        }

        message = default;
        return false;
    }

    internal void Write<TWriter>(TWriter writer) where TWriter : IPrimitiveWriter, allows ref struct
    {
        writer.Write(TopicName);
        writer.Write(PartitionCount);
    }

    void IProtocolPrimitive<CreateTopicMessage>.Write<TWriter>(TWriter writer) => Write(writer);
}