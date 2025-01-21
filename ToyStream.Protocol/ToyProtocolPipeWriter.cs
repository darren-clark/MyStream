namespace ToyStream.Protocol;

using System.IO.Pipelines;

public sealed class ToyProtocolPipeWriter(PipeWriter pipe): IToyProtocolWriter
{
    public ValueTask Write<T>(T message, CancellationToken cancellationToken) where T : struct, IMessage<T>, allows ref struct
    {
        PrimitiveWriter<PipeWriter> writer = new PrimitiveWriter<PipeWriter>(pipe);
        writer.Write(message.Type);
        message.Write(writer);

        return Flush();

        async ValueTask Flush() => await pipe.FlushAsync(cancellationToken).ConfigureAwait(false);
    }
}