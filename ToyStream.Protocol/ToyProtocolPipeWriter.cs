namespace ToyStream.Protocol.Send;

using System.IO.Pipelines;
using Shared;

public sealed class ToyProtocolPipeWriter(PipeWriter pipe): IToyProtocolWriter
{
    public ValueTask Write<T>(T message, CancellationToken cancellationToken) where T : struct, IMessage<T>, allows ref struct
    {
        BigEndianSpanWriter bigEndianSpanWriter = pipe.GetSpan(message.Size + sizeof(MessageType));
        bigEndianSpanWriter.Write(message.Type);
        message.Write(ref bigEndianSpanWriter);
        pipe.Advance(bigEndianSpanWriter.Written.Length);

        return Flush();

        async ValueTask Flush() => await pipe.FlushAsync(cancellationToken).ConfigureAwait(false);
    }
}