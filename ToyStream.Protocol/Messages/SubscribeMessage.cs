using ToyStream.Shared;

namespace ToyStream.Protocol.Messages;

public enum SubscribeOffset : byte
{
    Earliest,
    Latest
}

/// <summary>
/// One way: once the pump owns the write side there is no reader for an answer, so success looks
/// like records arriving and failure looks like <see cref="SubscriptionLostMessage"/>. One failure
/// path rather than two.
/// </summary>
public readonly ref struct SubscribeMessage : IRequestMessage<SubscribeMessage>
{
    public required RefUtf8String Topic { get; init; }

    public required SubscribeOffset StartOffset { get; init; }

    static bool IMessage<SubscribeMessage>.TryRead<TReader>(ref TReader reader, out SubscribeMessage message,
        out int bytesToRead)
    {
        var tryReader = reader;
        if (!tryReader.TryReadUtf8String(out var topic, out bytesToRead) ||
            !tryReader.TryReadEnum<SubscribeOffset>(out var startOffset, out bytesToRead))
        {
            message = default;
            return false;
        }

        reader = tryReader;
        message = new SubscribeMessage { Topic = topic, StartOffset = startOffset };
        return true;
    }

    void IMessage.Write<TWriter>(TWriter writer)
    {
        writer.Write(this.Topic);
        writer.Write(this.StartOffset);
    }

    public static MessageType Type => MessageType.Subscribe;
}
