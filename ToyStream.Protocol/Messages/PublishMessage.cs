using ToyStream.Protocol.Responses;
using ToyStream.Shared;

namespace ToyStream.Protocol.Messages;

/// <summary>
/// Header only: the body follows it on the wire and is streamed to a receiver, never buffered whole.
/// The body is the last field so the connection can hand the rest of the frame straight over.
/// </summary>
public readonly ref struct PublishMessage : IRequestMessage<PublishMessage, AcknowledgeMessage<PublishResult>>
{
    public required RefUtf8String Topic { get; init; }
    public required short Partition { get; init; }
    public required long BodyLength { get; init; }

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

        if (!tryReader.TryReadInt64(out var bodyLength, out bytesToRead))
        {
            message = default;
            return false;
        }

        reader = tryReader;
        message = new PublishMessage
        {
            Topic = topic,
            Partition = partition,
            BodyLength = bodyLength,
        };
        return true;
    }

    internal void Write<TWriter>(TWriter writer) where TWriter : IPrimitiveWriter, allows ref struct
    {
        writer.Write(Topic);
        writer.Write(Partition);
        writer.Write(BodyLength);
    }

    void IMessage.Write<TWriter>(TWriter writer) => Write(writer);
}
