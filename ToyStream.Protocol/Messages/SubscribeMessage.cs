using ToyStream.Shared;

namespace ToyStream.Protocol.Messages;

public enum SubscribeOffset : byte
{
    Earliest,
    Latest,

    /// <summary>Where this group last committed, or the earliest record if it never has.</summary>
    Committed
}

/// <summary>
/// One way: once the pump owns the write side there is no reader for an answer, so success looks
/// like records arriving and failure looks like <see cref="SubscriptionLostMessage"/>. One failure
/// path rather than two.
/// </summary>
public readonly ref struct SubscribeMessage : IRequestMessage<SubscribeMessage>
{
    public required RefUtf8String Topic { get; init; }

    /// <summary>Empty for a subscriber that keeps no checkpoint and shares none with anyone.</summary>
    public required RefUtf8String Group { get; init; }

    /// <summary>
    /// Sorts the group, so it must be stable across a restart or a returning subscriber reshuffles
    /// everyone instead of reclaiming its own partitions.
    /// </summary>
    public required RefUtf8String MemberId { get; init; }

    public required SubscribeOffset StartOffset { get; init; }

    static bool IMessage<SubscribeMessage>.TryRead<TReader>(ref TReader reader, out SubscribeMessage message,
        out int bytesToRead)
    {
        var tryReader = reader;
        if (!tryReader.TryReadUtf8String(out var topic, out bytesToRead) ||
            !tryReader.TryReadUtf8String(out var group, out bytesToRead) ||
            !tryReader.TryReadUtf8String(out var memberId, out bytesToRead) ||
            !tryReader.TryReadEnum<SubscribeOffset>(out var startOffset, out bytesToRead))
        {
            message = default;
            return false;
        }

        reader = tryReader;
        message = new SubscribeMessage
        {
            Topic = topic, Group = group, MemberId = memberId, StartOffset = startOffset
        };
        return true;
    }

    void IMessage.Write<TWriter>(TWriter writer)
    {
        writer.Write(this.Topic);
        writer.Write(this.Group);
        writer.Write(this.MemberId);
        writer.Write(this.StartOffset);
    }

    public static MessageType Type => MessageType.Subscribe;
}
