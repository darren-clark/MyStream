namespace ToyStream.Protocol;

using System.Buffers;
using System.IO.Pipelines;
using System.Runtime.InteropServices;
using Messages;
using Responses;

public class ToyProtocolPipeReader : IToyProtocolReader
{
    private readonly PipeReader pipe;

    public ToyProtocolPipeReader(PipeReader pipe)
    {
        this.pipe = pipe;
#if DEBUG
        this.tag = "<empty>";
#endif
    }

#if DEBUG
    private readonly string tag;

    public ToyProtocolPipeReader(PipeReader reader, string tag) : this(reader)
    {
        this.tag = tag;
    }
#endif

    public async Task Read(IToyProtocolHandler handler, CancellationToken cancellationToken)
    {
        await Task.Yield();
        var buffer = ReadOnlySequence<byte>.Empty;

        try
        {
            while (!cancellationToken.IsCancellationRequested)
            {
// Disable "should await immediately" warning. It's being awaited outside the switch expression.
#pragma warning disable CA2012
                await (await ReadMessageType().ConfigureAwait(false) switch
                {
                    // Ugh, you can't pattern match on ref structs, see https://github.com/dotnet/runtime/issues/65112 and
                    // https://github.com/dotnet/runtime/issues/105435
                    // So for now we'll have a handler method for each, even though servers and clients are
                    // completely different.
                    MessageType.CreateTopic => HandleMessage<CreateTopicMessage>(m =>
                        handler.HandleCreateTopicMessage(in m)),
                    MessageType.Acknowledge => HandleResponseMessage<AcknowledgeMessage>(),
                    _ => throw new NotImplementedException()
                }).ConfigureAwait(false);
            }
        }
        catch (OperationCanceledException)
        {
        }
#pragma warning restore CA2012
        return;

        async ValueTask<MessageType> ReadMessageType()
        {
            if (buffer.Length < sizeof(MessageType))
            {
                var result = await this.pipe.ReadAsync(cancellationToken).ConfigureAwait(false);
                buffer = result.Buffer;
            }

            var messageBuffer = buffer.Slice(0, 1);
            var messageType = (MessageType)MemoryMarshal.Read<byte>(messageBuffer.FirstSpan);
            buffer = buffer.Slice(messageBuffer.End);
            return messageType;
        }


        async ValueTask HandleResponseMessage<T>() where T : struct, IResponseMessage<T>
        {
            var reader = new PrimitiveReader(buffer);
            var read = T.TryRead(ref reader, out var message, out var bytesToRead);

            while (!read)
            {
                this.pipe.AdvanceTo(buffer.Start, buffer.End);
                var readResult = await this.pipe.ReadAtLeastAsync(bytesToRead, cancellationToken).ConfigureAwait(false);
                buffer = readResult.Buffer;
                reader = new PrimitiveReader(buffer);
                read = T.TryRead(ref reader, out message, out bytesToRead);
            }

            buffer = reader.Remaining;
            this.pipe.AdvanceTo(reader.Remaining.Start);
            await handler.HandleResponseMessage(in message).ConfigureAwait(false);
        }

        async ValueTask HandleMessage<T>(Func<T, ValueTask> messageHandler)
            where T : struct, IMessage<T>, allows ref struct
        {
            // Have to duplicate the reading code here, because we
            // can't return a ValueTask<T> from shared code.
            var reader = new PrimitiveReader(buffer);
            var read = T.TryRead(ref reader, out var message, out var bytesToRead);

            while (!read)
            {
                this.pipe.AdvanceTo(buffer.Start, buffer.End);
                var readResult = await this.pipe.ReadAtLeastAsync(bytesToRead, cancellationToken).ConfigureAwait(false);
                buffer = readResult.Buffer;
                reader = new PrimitiveReader(buffer);
                read = T.TryRead(ref reader, out message, out bytesToRead);
            }

            buffer = reader.Remaining;
            this.pipe.AdvanceTo(reader.Remaining.Start);
            await messageHandler(message).ConfigureAwait(false);
        }
    }
}