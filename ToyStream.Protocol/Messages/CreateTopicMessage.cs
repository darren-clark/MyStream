using ToyStream.Protocol.Responses;

namespace ToyStream.Protocol.Messages;

using Shared;

public readonly ref struct
    CreateTopicMessage : IRequestMessage<CreateTopicMessage, AcknowledgeMessage<CreateTopicResult>>
{
    public required RefUtf8String TopicName { get; init; }
    public required short PartitionCount { get; init; }

    public static MessageType Type => MessageType.CreateTopic;

    static bool IMessage<CreateTopicMessage>.TryRead<TReader>(ref TReader reader, out CreateTopicMessage message,
        out int bytesToRead)
    {
        var tryReader = reader;
        if (!tryReader.TryReadUtf8String(out var topicName, out bytesToRead) ||
            !tryReader.TryReadInt16(out var partitionCount, out bytesToRead))
        {
            message = default;
            return false;
        }

        reader = tryReader;
        message = new CreateTopicMessage
        {
            TopicName = topicName,
            PartitionCount = partitionCount
        };
        return true;
    }

    internal void Write<TWriter>(TWriter writer) where TWriter : IPrimitiveWriter, allows ref struct
    {
        writer.Write(TopicName);
        writer.Write(PartitionCount);
    }

    void IMessage.Write<TWriter>(TWriter writer) => Write(writer);
}