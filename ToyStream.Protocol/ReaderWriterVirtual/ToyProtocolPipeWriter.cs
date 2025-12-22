namespace ToyStream.Protocol.ReaderWriterVirtual;

using System.IO.Pipelines;

public sealed class ToyProtocolPipeWriter(PipeWriter pipe): IToyProtocolWriter
{
    public ValueTask Write<T>(T message, CancellationToken cancellationToken) where T : struct, IMessage<T>, allows ref struct
    {
        var writer = new PrimitiveWriter<PipeWriter>(pipe);
        writer.Write(message.Type);
        message.Write(writer);

        return Flush();

        async ValueTask Flush() => await pipe.FlushAsync(cancellationToken).ConfigureAwait(false);
    }
    
    public ValueTask WriteResponse<T>(T message, CancellationToken cancellationToken) where T : struct, IMessage<T>, allows ref struct
    {
        var writer = new PrimitiveWriter<PipeWriter>(pipe);
        writer.Write((byte) ((byte)message.Type & 0x40));
        message.Write(writer);

        return Flush();

        async ValueTask Flush() => await pipe.FlushAsync(cancellationToken).ConfigureAwait(false);
    }

}