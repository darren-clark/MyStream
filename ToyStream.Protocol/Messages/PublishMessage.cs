using System.IO.Pipelines;
using ToyStream.Protocol.Responses;
using ToyStream.Shared;

namespace ToyStream.Protocol.Messages;

/// <summary>
/// Header only. The body follows as length-prefixed chunks owned by the streaming layer, so nothing
/// here describes its size - a sender that does not know it up front can still send one.
/// </summary>
public readonly ref struct PublishMessage
    : IStreamingRequestMessage<PublishMessage, AcknowledgeMessage<PublishResult>>
{
    public PublishMessage()
    {
    }

    public required RefUtf8String Topic { get; init; }
    public required short Partition { get; init; }

    /// <summary>Send side only; a received body is streamed to a receiver instead.</summary>
    public PipeReader Body { get; init; } = null!;

    /// <summary>Lets a known-length body go out as one chunk; null streams it as it arrives.</summary>
    public int? Size { get; init; }

    public static MessageType Type => MessageType.PublishEvent;

    static bool IMessage<PublishMessage>.TryRead<TReader>(ref TReader reader, out PublishMessage message,
        out int bytesToRead)
    {
        var tryReader = reader;
        if (!tryReader.TryReadUtf8String(out var topic, out bytesToRead))
        {
            message = default;
            return false;
        }

        if (!tryReader.TryReadInt16(out var partition, out bytesToRead))
        {
            message = default;
            return false;
        }

        reader = tryReader;
        message = new PublishMessage
        {
            Topic = topic,
            Partition = partition,
        };
        return true;
    }

    internal void Write<TWriter>(TWriter writer) where TWriter : IPrimitiveWriter, allows ref struct
    {
        writer.Write(Topic);
        writer.Write(Partition);
    }

    void IMessage.Write<TWriter>(TWriter writer) => Write(writer);
}
