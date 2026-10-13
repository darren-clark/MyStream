using System.IO.Pipelines;
using ToyStream.Protocol.Responses;
using ToyStream.Shared;

namespace ToyStream.Protocol.Messages;

/// <summary>
/// Header only. The body follows as length-prefixed chunks owned by the streaming layer. The header
/// declares the total size when the sender knows it, but the chunk framing - not this - decides where
/// the body ends, so a sender that does not know the size up front can still send one.
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

    /// <summary>
    /// Total body length when known, letting the receiver size its buffer up front; null streams it
    /// as it arrives. Advisory only - it never determines where the body ends.
    /// </summary>
    public long? Size { get; init; }

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

        if (!tryReader.TryReadInt64(out var size, out bytesToRead))
        {
            message = default;
            return false;
        }

        reader = tryReader;
        message = new PublishMessage
        {
            Topic = topic,
            Partition = partition,
            Size = size < 0 ? null : size,
        };
        return true;
    }

    internal void Write<TWriter>(TWriter writer) where TWriter : IPrimitiveWriter, allows ref struct
    {
        writer.Write(Topic);
        writer.Write(Partition);
        writer.Write(Size is { } size and >= 0 ? size : -1L);
    }

    void IMessage.Write<TWriter>(TWriter writer) => Write(writer);
}
