using ToyStream.Shared;

namespace ToyStream.Protocol.Messages;

public enum SubscribeOffset : byte
{
    Stored,
    Earliest,
    Latest
}

public readonly ref struct SubscribeMessage : IRequestMessage<SubscribeMessage, SubscribeResponseMessage>
{
    public required RefUtf8String ConsumerGroup { get; init; }
    public required RefUtf8String Topic { get; init; }
    public required SubscribeOffset StartOffset { get; init; }

    static bool IMessage<SubscribeMessage>.TryRead<TReader>(ref TReader reader, out SubscribeMessage message,
        out int bytesToRead)
    {
        var tryReader = reader;
        if (!tryReader.TryReadUtf8String(out var topic, out bytesToRead) ||
            !tryReader.TryReadUtf8String(out var consumerGroup, out bytesToRead) ||
            !tryReader.TryReadEnum<SubscribeOffset>(out var startOffset, out bytesToRead))
        {
            message = default;
            return false;
        }

        reader = tryReader;
        message = new SubscribeMessage { Topic = topic, ConsumerGroup = consumerGroup, StartOffset = startOffset };
        return true;
    }

    public void Write<TWriter>(TWriter writer) where TWriter : struct, IPrimitiveWriter, allows ref struct
    {
        writer.Write(Topic);
        writer.Write(ConsumerGroup);
        writer.Write(StartOffset);
    }

    public static MessageType Type => MessageType.Subscribe;
}