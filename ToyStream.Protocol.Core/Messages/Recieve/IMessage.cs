namespace ToyStream.Protocol.Messages.Recieve;

public interface IMessage<T> where T: struct, IMessage<T>, allows ref struct
{
    public static abstract bool TryRead(ref ProtocolPrimitiveReader reader, out T message, out int bytesToRead);
}