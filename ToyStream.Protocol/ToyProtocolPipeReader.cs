namespace ToyStream.Protocol;

using System.Buffers;
using System.IO.Pipelines;
using System.Runtime.CompilerServices;
using System.Runtime.InteropServices;
using Messages;
using Responses;

public class ToyProtocolPipeReader : IToyProtocolReader
{
    private readonly PipeReader pipe;

    // Passing the handler call as a type rather than a delegate keeps the read loop allocation free:
    // a closure over 'handler' cannot be cached, but a struct type argument costs nothing.
    private interface IMessageDispatcher<TMessage>
        where TMessage : struct, IMessage<TMessage>, allows ref struct
    {
        static abstract ValueTask Dispatch(IToyProtocolHandler handler, ref readonly TMessage message);
    }

    private readonly struct CreateTopicDispatcher : IMessageDispatcher<CreateTopicMessage>
    {
        public static ValueTask Dispatch(IToyProtocolHandler handler, ref readonly CreateTopicMessage message)
            => handler.HandleCreateTopicMessage(in message);
    }

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
                    MessageType.CreateTopic => HandleMessage<CreateTopicMessage, CreateTopicDispatcher>(),
                    MessageType.Acknowledge => HandleResponseMessage<AcknowledgeMessage, AcknowledgeResponse>(),
                    _ => throw new NotImplementedException()
                }).ConfigureAwait(false);
            }
        }
        catch (OperationCanceledException)
        {
        }
#pragma warning restore CA2012
        return;

        [AsyncMethodBuilder(typeof(PoolingAsyncValueTaskMethodBuilder<>))]
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


        [AsyncMethodBuilder(typeof(PoolingAsyncValueTaskMethodBuilder))]
        async ValueTask HandleResponseMessage<TResponseMessage, TResponse>()
            where TResponseMessage : struct, IResponseMessage<TResponseMessage>, allows ref struct
            where TResponse : class, IResponse<TResponseMessage, TResponse>
        {
            var reader = new PrimitiveReader(buffer);
            var read = TResponseMessage.TryRead(ref reader, out var message, out var bytesToRead);

            while (!read)
            {
                this.pipe.AdvanceTo(buffer.Start, buffer.End);
                var readResult = await this.pipe.ReadAtLeastAsync(bytesToRead, cancellationToken).ConfigureAwait(false);
                buffer = readResult.Buffer;
                reader = new PrimitiveReader(buffer);
                read = TResponseMessage.TryRead(ref reader, out message, out bytesToRead);
            }

            buffer = reader.Remaining;
            await handler.HandleResponseMessage<TResponseMessage, TResponse>(in message).ConfigureAwait(false);
            this.pipe.AdvanceTo(buffer.Start);
        }

        [AsyncMethodBuilder(typeof(PoolingAsyncValueTaskMethodBuilder))]
        async ValueTask HandleMessage<TMessage, TDispatcher>()
            where TMessage : struct, IMessage<TMessage>, allows ref struct
            where TDispatcher : struct, IMessageDispatcher<TMessage>
        {
            // Have to duplicate the reading code here, because we
            // can't return a ValueTask<T> from shared code.
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

            // Dispatch before advancing: the message is a view over the pipe buffer, and AdvanceTo
            // hands those segments back to the pipe to be reused.
            await TDispatcher.Dispatch(handler, in message).ConfigureAwait(false);
            this.pipe.AdvanceTo(buffer.Start);
        }
    }
}