namespace ToyStream.Protocol.Messages.Send;

using System.Buffers;
using Shared;

public readonly ref struct AcknowledgeMessage: IMessage
{
    public int Size => 0;
    public MessageType Type => MessageType.Acknowledge;

    public void Write<T>(ref ProtocolPrimitiveWriter<T> primitiveWriter) where T : IBufferWriter<byte>
    {
    }
}