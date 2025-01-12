namespace ToyStream.Protocol.Send;

using System.IO.Pipelines;
using Shared;

public interface IToyProtocolWriter
{
    ValueTask Write<T>(T message, CancellationToken cancellationToken) where T : struct, IMessage, allows ref struct;
}

public sealed class ToyProtocolWriter(PipeWriter writer): IToyProtocolWriter
{
    public ValueTask Write<T>(T message, CancellationToken cancellationToken) where T : struct, IMessage, allows ref struct
    {
        SpanWriter spanWriter = writer.GetSpan(message.Size + sizeof(MessageType));
        spanWriter.Write(message.Type);
        message.Write(ref spanWriter);
        writer.Advance(spanWriter.Written.Length);

        return Flush();

        async ValueTask Flush() => await writer.FlushAsync(cancellationToken).ConfigureAwait(false);
    }
}