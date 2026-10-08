namespace ToyStream.Protocol.Dispatch;

using System.Buffers;
using System.IO.Pipelines;
using System.Runtime.CompilerServices;
using System.Runtime.InteropServices;
using Messages;
using Responses;

/// <summary>
/// Dispatch variant: the handler is a struct type argument, so the call is a constrained call the
/// JIT can resolve statically. Unlike a class type argument the instantiation is not shared, which
/// is what makes devirtualization possible.
/// Body is a deliberate copy of <see cref="VirtualConnection"/>; only the dispatch line differs.
/// </summary>
public sealed class StructHandlerConnection<THandler> : IAsyncDisposable
    where THandler : struct, IMessageHandler
{
    private readonly PipeReader reader;
    private readonly PipeWriter writer;
    private readonly CancellationToken cancellationToken;
    private readonly ResponseCompletionSourceCache cachedCompletionSources = new();
    private readonly Task readerTask;

    // Deliberately not readonly: a readonly field of an unconstrained struct type forces a
    // defensive copy on every call, which is what this variant is trying to avoid.
    private THandler handler;

    private ResponseCompletionSource? responseCompletionSource;

    public StructHandlerConnection(PipeReader reader, PipeWriter writer, THandler handler,
        CancellationToken cancellationToken)
    {
        this.reader = reader;
        this.writer = writer;
        this.handler = handler;
        this.cancellationToken = cancellationToken;
        this.readerTask = this.Read();
    }

    public ValueTask<TResponse> SendMessage<TRequestMessage, TResponseMessage, TResponse>(
        TRequestMessage requestMessage, CancellationToken sendCancellationToken)
        where TRequestMessage : struct, IRequestMessage<TRequestMessage, TResponseMessage>, allows ref struct
        where TResponseMessage : struct, IResponseMessage<TResponseMessage>, allows ref struct
        where TResponse : class, IResponse<TResponseMessage, TResponse>
    {
        var completionSource = this.cachedCompletionSources.Get<TResponse>();
        this.responseCompletionSource = completionSource;

        return SendAndWait(
            this.Write(requestMessage, sendCancellationToken),
            completionSource,
            sendCancellationToken.Register(static c => ((ResponseCompletionSource<TResponse>)c!).Cancel(), completionSource));

        [AsyncMethodBuilder(typeof(PoolingAsyncValueTaskMethodBuilder<>))]
        static async ValueTask<TResponse> SendAndWait(ValueTask writerTask,
            ResponseCompletionSource<TResponse> responseCompletion, CancellationTokenRegistration cancellation)
        {
            await writerTask.ConfigureAwait(false);
            var result = await responseCompletion.ValueTask.ConfigureAwait(false);
            await cancellation.DisposeAsync();
            return result;
        }
    }

    private ValueTask Write<TMessage>(TMessage message, CancellationToken writeCancellationToken)
        where TMessage : struct, IMessage<TMessage>, allows ref struct
    {
        PrimitiveWriter<PipeWriter> primitiveWriter = new(this.writer);
        primitiveWriter.Write(TMessage.Type);
        message.Write(primitiveWriter);

        return Flush();

        [AsyncMethodBuilder(typeof(PoolingAsyncValueTaskMethodBuilder))]
        async ValueTask Flush() => await this.writer.FlushAsync(writeCancellationToken).ConfigureAwait(false);
    }

    private static bool TryReadMessage<TMessage>(ref PrimitiveReader reader, out TMessage message, out int bytesToRead)
        where TMessage : struct, IMessage<TMessage>, allows ref struct
        => TMessage.TryRead(ref reader, out message, out bytesToRead);

    private static TResponse CreateResponse<TResponseMessage, TResponse>(ref readonly TResponseMessage message)
        where TResponseMessage : struct, IResponseMessage<TResponseMessage>, allows ref struct
        where TResponse : class, IResponse<TResponseMessage, TResponse>
        => TResponse.CreateResponse(in message);

    private async Task Read()
    {
        await Task.Yield();
        var buffer = ReadOnlySequence<byte>.Empty;

        try
        {
            while (!this.cancellationToken.IsCancellationRequested)
            {
// Awaited immediately, just outside the switch expression.
#pragma warning disable CA2012
                await (await ReadMessageType().ConfigureAwait(false) switch
                {
                    MessageType.CreateTopic => DispatchCreateTopic(),
                    MessageType.Acknowledge => DispatchAcknowledge(),
                    _ => throw new NotImplementedException()
                }).ConfigureAwait(false);
#pragma warning restore CA2012
            }
        }
        catch (OperationCanceledException)
        {
        }

        return;

        [AsyncMethodBuilder(typeof(PoolingAsyncValueTaskMethodBuilder<>))]
        async ValueTask<MessageType> ReadMessageType()
        {
            if (buffer.Length < sizeof(MessageType))
            {
                var result = await this.reader.ReadAsync(this.cancellationToken).ConfigureAwait(false);
                buffer = result.Buffer;
            }

            var messageBuffer = buffer.Slice(0, 1);
            var messageType = (MessageType)MemoryMarshal.Read<byte>(messageBuffer.FirstSpan);
            buffer = buffer.Slice(messageBuffer.End);
            return messageType;
        }

        [AsyncMethodBuilder(typeof(PoolingAsyncValueTaskMethodBuilder))]
        async ValueTask DispatchCreateTopic()
        {
            var primitiveReader = new PrimitiveReader(buffer);
            var read = TryReadMessage(ref primitiveReader, out CreateTopicMessage message, out var bytesToRead);

            while (!read)
            {
                this.reader.AdvanceTo(buffer.Start, buffer.End);
                var readResult = await this.reader.ReadAtLeastAsync(bytesToRead, this.cancellationToken).ConfigureAwait(false);
                buffer = readResult.Buffer;
                primitiveReader = new PrimitiveReader(buffer);
                read = TryReadMessage(ref primitiveReader, out message, out bytesToRead);
            }

            buffer = primitiveReader.Remaining;

            // The only line that differs between dispatch variants.
            await this.handler.HandleMessage<CreateTopicMessage, AcknowledgeMessage>(in message).ConfigureAwait(false);

            // Advance only after dispatch: the message is a view over the pipe buffer, and
            // AdvanceTo hands those segments back to the pipe to be reused.
            this.reader.AdvanceTo(buffer.Start);

            // Identical in every variant, so the response path cannot bias the comparison.
            await this.Write(new AcknowledgeMessage(), this.cancellationToken).ConfigureAwait(false);
        }

        [AsyncMethodBuilder(typeof(PoolingAsyncValueTaskMethodBuilder))]
        async ValueTask DispatchAcknowledge()
        {
            var primitiveReader = new PrimitiveReader(buffer);
            var read = TryReadMessage(ref primitiveReader, out AcknowledgeMessage message, out var bytesToRead);

            while (!read)
            {
                this.reader.AdvanceTo(buffer.Start, buffer.End);
                var readResult = await this.reader.ReadAtLeastAsync(bytesToRead, this.cancellationToken).ConfigureAwait(false);
                buffer = readResult.Buffer;
                primitiveReader = new PrimitiveReader(buffer);
                read = TryReadMessage(ref primitiveReader, out message, out bytesToRead);
            }

            buffer = primitiveReader.Remaining;
            this.responseCompletionSource!.TrySetResult(
                CreateResponse<AcknowledgeMessage, AcknowledgeResponse>(in message));
            this.reader.AdvanceTo(buffer.Start);
        }
    }

    public async ValueTask DisposeAsync()
    {
        await this.readerTask.ConfigureAwait(false);
        GC.SuppressFinalize(this);
    }
}
