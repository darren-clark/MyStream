namespace ToyStream.Protocol.Messages;

using Recieve;

public readonly ref struct AcknowledgeMessage: IMessage<AcknowledgeMessage>
{
    public static bool TryRead(ref ProtocolPrimitiveReader reader, out AcknowledgeMessage message, out int bytesToRead)
    {
        message = new AcknowledgeMessage();
        bytesToRead = 0;
        return true;
    }
}