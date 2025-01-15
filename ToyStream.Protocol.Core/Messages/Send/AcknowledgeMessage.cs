namespace ToyStream.Protocol.Messages;

using System.Buffers;
using Shared;

public readonly ref struct AcknowledgeMessage: IMessage<AcknowledgeMessage>
{
    public AcknowledgeMessage()
    {
    }

    public int Size => 0;
    public MessageType Type => MessageType.Acknowledge;

    public static bool TryRead(scoped ref SequenceReader<byte> reader, out AcknowledgeMessage message, out int bytesToRead)
    {
        message = new AcknowledgeMessage();
        bytesToRead = 0;
        return true;
    }

    public void Write(ref BigEndianSpanWriter writer) { }
}