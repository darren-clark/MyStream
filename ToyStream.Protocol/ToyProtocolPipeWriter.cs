namespace ToyStream.Protocol;

using System.IO.Pipelines;
using System.Runtime.CompilerServices;

public sealed class ToyProtocolPipeWriter(PipeWriter pipe): IToyProtocolWriter
{
    public ValueTask Write<T>(T message, CancellationToken cancellationToken) where T : struct, IMessage<T>, allows ref struct
    {
        PrimitiveWriter<PipeWriter> writer = new PrimitiveWriter<PipeWriter>(pipe);
        writer.Write(T.Type);
        message.Write(writer);

        return Flush();

        [AsyncMethodBuilder(typeof(PoolingAsyncValueTaskMethodBuilder))]
        async ValueTask Flush() => await pipe.FlushAsync(cancellationToken).ConfigureAwait(false);
    }
}