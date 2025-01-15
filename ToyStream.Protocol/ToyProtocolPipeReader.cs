namespace ToyStream.Protocol;

using System.Buffers;
using System.IO.Pipelines;
using System.Runtime.InteropServices;
using Messages;

public class ToyProtocolPipeReader
{
    private readonly PipeReader pipe;
    
    public ToyProtocolPipeReader(PipeReader pipe)
    {
        this.pipe = pipe;
    }
    
#if DEBUG
    private readonly string tag;
    
    public ToyProtocolPipeReader(PipeReader reader, string tag):this(reader)
    {
        this.tag = tag;
    }
#endif
    
    public async Task Read(IToyProtocolHandler handler, CancellationToken cancellationToken)
    {
        await Task.Yield();
        var buffer = ReadOnlySequence<byte>.Empty;

        while (!cancellationToken.IsCancellationRequested)
        {
// Disable "should await immediately" warning. It's being awaited outside the switch expression.
#pragma warning disable CA2012
            await (await ReadMessageType().ConfigureAwait(false) switch
            {
                MessageType.Acknowledge => HandleMessage<AcknowledgeMessage>(),
                MessageType.CreateTopic => HandleMessage<CreateTopicMessage>(),
                _ => throw new NotImplementedException()
            }).ConfigureAwait(false);
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
            
        async ValueTask HandleMessage<T>() where T : struct, IMessage<T>, allows ref struct
        {
            var bufferReader = new SequenceReader<byte>(buffer);
            var read = T.TryRead(ref bufferReader, out var message, out var bytesToRead);
            while (!read)
            {
                this.pipe.AdvanceTo(buffer.Start, buffer.End);
                var readResult = await this.pipe.ReadAtLeastAsync(bytesToRead, cancellationToken).ConfigureAwait(false);
                buffer = readResult.Buffer;
                bufferReader = new SequenceReader<byte>(readResult.Buffer);
                read = T.TryRead(ref bufferReader, out message, out bytesToRead);
            }

            await handler.HandleMessage(in message).ConfigureAwait(false);
        }
    }
}