namespace ToyStream.Protocol.ReceiveRef;

using System.Buffers;

public readonly ref struct AcknowledgeMessage: IMessage<AcknowledgeMessage>
{
    public static bool TryRead(scoped ref SequenceReader<byte> reader, out AcknowledgeMessage component, out int size)
    {
        component = new AcknowledgeMessage();
        size = 0;
        return true;
    }
}