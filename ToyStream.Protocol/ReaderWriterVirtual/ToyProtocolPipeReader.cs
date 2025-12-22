namespace ToyStream.Protocol.ReaderWriterVirtual;

using System.Buffers;
using System.IO.Pipelines;
using System.Runtime.InteropServices;
using Messages;
using Shared;

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

    private delegate ValueTask ProcessDelegate<TMessage>(ref readonly TMessage message)
        where TMessage : struct, IMessage<TMessage>, allows ref struct;
    
    public async Task Read(IToyProtocolHandler handler, CancellationToken cancellationToken)
    {
        await Task.Yield();
        var buffer = ReadOnlySequence<byte>.Empty;

        try
        {
            while (!cancellationToken.IsCancellationRequested)
            {
                if (buffer.Length < sizeof(MessageType))
                {
                    var result = await this.pipe.ReadAsync(cancellationToken).ConfigureAwait(false);
                    buffer = result.Buffer;
                }

                var messageBuffer = buffer.Slice(0, 1);
                var messageTypeByte = MemoryMarshal.Read<byte>(messageBuffer.FirstSpan);
                buffer = buffer.Slice(messageBuffer.End);

                // Is high bit set?
                // If so, it is a response message.
                if ((messageTypeByte & 0x40) != 0)
                {
                    var messageType = (MessageType) (messageTypeByte & 0x3F);
                    await (messageType switch
                    {
                        MessageType.CreateTopic => ReadAndProcess((ref readonly CreateTopicResponseMessage message) =>
                                handler.HandleResponseMessage<CreateTopicResponseMessage, CreateTopicResponse>(in message)),
                        _ => ThrowHelpers.ThrowNotImplemented<ValueTask>()
                        }).ConfigureAwait(false);
                    
                }
                else
                {
                    var messageType = (MessageType) messageTypeByte;
                    await (messageType switch
                    {
                        MessageType.CreateTopic => ReadAndProcess((ref readonly CreateTopicMessage message) =>
                                handler.HandleCreateTopicMessage(in message)),
                        _ => ThrowHelpers.ThrowNotImplemented<ValueTask>()
                        }).ConfigureAwait(false);
                }
            }
        }
        catch (OperationCanceledException)
        {
        }
        
        return;

    
        async ValueTask ReadAndProcess<TMessage>(ProcessDelegate<TMessage> messageHandler)
            where TMessage : struct, IMessage<TMessage>, allows ref struct
        {
            var reader = new PrimitiveReader(buffer);
            var read = TMessage.TryRead(ref reader, out var message, out var bytesToRead);

            while (!read)
            {
                this.pipe.AdvanceTo(buffer.Start, buffer.End);
                var readResult = await this.pipe.ReadAtLeastAsync(bytesToRead, cancellationToken).ConfigureAwait(false);
                buffer = readResult.Buffer;
                reader = new PrimitiveReader(buffer);
                read = TMessage.TryRead(ref reader, out message, out bytesToRead);
            }

            buffer = reader.Remaining;
            this.pipe.AdvanceTo(reader.Remaining.Start);
            await messageHandler(in message).ConfigureAwait(false);
        }
    }
}