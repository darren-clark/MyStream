namespace ToyStream.Protocol.ReceiveRef;

using System.Buffers;
using System.IO.Pipelines;
using System.Runtime.InteropServices;

public interface IToyProtocolReader
{
    Task Read(IToyProtocolHandler handler, CancellationToken cancellationToken);
}

public sealed class ToyProtocolReader: IToyProtocolReader
{
    private readonly PipeReader reader;
    
    public ToyProtocolReader(PipeReader reader)
    {
        this.reader = reader;
    }

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
                var result = await this.reader.ReadAsync(cancellationToken).ConfigureAwait(false);
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
            T message;
            int size;
            while(!T.TryRead(ref bufferReader, out message, out size))
            {
                this.reader.AdvanceTo(buffer.Start, buffer.End);
                var read = await this.reader.ReadAtLeastAsync(size, cancellationToken).ConfigureAwait(false);
                buffer = read.Buffer;
                bufferReader = new SequenceReader<byte>(read.Buffer);
            }

            bufferReader.Advance(size);
            buffer = bufferReader.UnreadSequence;
            
            this.reader.AdvanceTo(bufferReader.UnreadSequence.Start);
            
            await handler.HandleMessage(in message).ConfigureAwait(false);
        }
    }
}