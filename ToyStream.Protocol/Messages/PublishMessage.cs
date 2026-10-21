using ToyStream.Protocol.Responses;
using ToyStream.Shared;

namespace ToyStream.Protocol.Messages;

/// <summary>
/// Header only: the body follows as length-prefixed chunks, carried by
/// <see cref="StreamingMessage{TMessage}"/> rather than described here.
/// </summary>
public readonly ref struct PublishMessage
    : IStreamingRequestMessage<PublishMessage, AcknowledgeMessage<PublishResult>>
{
    public required RefUtf8String Topic { get; init; }
    public required short Partition { get; init; }

    /// <summary>
    /// Opaque to the protocol: the sender picks the partition, so this is carried for the consumer
    /// rather than used for routing. Bounded, and read before the body so it never becomes a second
    /// stream.
    /// </summary>
    public RefBytes Key { get; init; }

    public static MessageType Type => MessageType.PublishEvent;

    static bool IMessage<PublishMessage>.TryRead<TReader>(ref TReader reader, out PublishMessage message,
        out int bytesToRead)
    {
        var tryReader = reader;
        if (!tryReader.TryReadUtf8String(out var topic, out bytesToRead) ||
            !tryReader.TryReadInt16(out var partition, out bytesToRead) ||
            !tryReader.TryReadBytes(ProtocolConstants.MaxKeySize, out var key, out bytesToRead))
        {
            message = default;
            return false;
        }

        reader = tryReader;
        message = new PublishMessage
        {
            Topic = topic,
            Partition = partition,
            Key = key,
        };
        return true;
    }

    internal void Write<TWriter>(TWriter writer) where TWriter : IPrimitiveWriter, allows ref struct
    {
        writer.Write(Topic);
        writer.Write(Partition);
        writer.Write(Key, ProtocolConstants.MaxKeySize);
    }

    void IMessage.Write<TWriter>(TWriter writer) => Write(writer);
}