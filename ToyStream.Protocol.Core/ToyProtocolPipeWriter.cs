namespace ToyStream.Protocol.Send;

using System.IO.Pipelines;
using Messages.Send;

public sealed class ToyProtocolPipeWriter(PipeWriter pipe): IToyProtocolWriter
{
    public ValueTask Write<T>(T message, CancellationToken cancellationToken) where T : struct, IMessage, allows ref struct
    {
        Messages.Send.ProtocolPrimitiveWriter<PipeWriter> writer = new(pipe);
        writer.Write(message.Type);
        message.Write(writer);

        return Flush();

        async ValueTask Flush() => await pipe.FlushAsync(cancellationToken).ConfigureAwait(false);
    }
}