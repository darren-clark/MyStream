namespace ToyStream.Protocol.Test.Messages;

using System.Buffers;

internal static class ProtocolTestHelpers
{

    public static bool TryReadMessage<TMessage>(ref PrimitiveReader reader, out TMessage message, out int bytesToRead)
        where TMessage : struct, IMessage<TMessage>, allows ref struct
        => TMessage.TryRead(ref reader, out message, out bytesToRead);
    
    public static void GetWriteBuffer(out ArrayBufferWriter<byte> buffer, out PrimitiveWriter<ArrayBufferWriter<byte>> writer, int size = 1024)
    {
        buffer = new (initialCapacity: 1024);
        writer = new (buffer);
    }
   
    public static void GetReadBuffer(out Span<byte> buffer, out PrimitiveReader reader, int size = 1024)
    {
        var array = new byte[size];
        buffer = array;
        reader = new(new ReadOnlySequence<byte>(array));
    }
    

}