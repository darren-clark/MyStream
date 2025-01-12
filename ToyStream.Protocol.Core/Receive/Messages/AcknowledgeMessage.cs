namespace ToyStream.Protocol.Receive;

using System.Buffers;
using System.Diagnostics.CodeAnalysis;

public struct AcknowledgeMessage: IMessage, IProtocolComponent<AcknowledgeMessage>
{
    public void Dispose()
    {
    }

    public static bool TryRead(ref SequenceReader<byte> reader, [NotNullWhen(true)] out AcknowledgeMessage? component, out int bytesToRead)
    {
        component = new AcknowledgeMessage();
        bytesToRead = 0;
        return true;
    }
}