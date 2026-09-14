namespace ToyStream.Protocol.Dispatch;

using System.Buffers;
using System.IO.Pipelines;
using System.Runtime.CompilerServices;
using System.Runtime.InteropServices;
using Messages;
using Responses;

/// <summary>
/// Message representation variant: the decoded message is copied onto the heap before the handler
/// sees it, instead of being passed as a view over the pipe buffer.
/// Body is a deliberate copy of <see cref="VirtualConnection"/>; only the materialisation of the
/// request message differs.
/// </summary>
public abstract class HeapMessageConnection : IAsyncDisposable
{
    private readonly PipeReader reader;
    private readonly PipeWriter writer;
    private readonly CancellationToken cancellationToken;
    private readonly ResponseCompletionSourceCache cachedCompletionSources = new();
    private readonly Task readerTask;

    private ResponseCompletionSource? responseCompletionSource;

    protected HeapMessageConnection(PipeReader reader, PipeWriter writer, CancellationToken cancellationToken)
    {
        this.reader = reader;
        this.writer = writer;
        this.cancellationToken = cancellationToken;
        this.readerTask = this.Read();
    }

    protected abstract ValueTask HandleCreateTopicMessage(HeapCreateTopicMessage message);

    protected ValueTask<TResponse> SendMessage<TRequestMessage, TResponseMessage, TResponse>(
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

    protected ValueTask Write<TMessage>(TMessage message, CancellationToken writeCancellationToken)
        where TMessage : struct, IMessage<TMessage>, allows ref struct
    {
        PrimitiveWriter<PipeWriter> primitiveWriter = new(this.writer);
        primitiveWriter.Write(message.Type);
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
            this.reader.AdvanceTo(primitiveReader.Remaining.Start);

            // The only thing that differs from VirtualConnection: the payload is copied off the
            // pipe buffer so the handler can be given something that outlives it.
            var heapMessage = new HeapCreateTopicMessage(in message);
            await this.HandleCreateTopicMessage(heapMessage).ConfigureAwait(false);

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
            this.reader.AdvanceTo(primitiveReader.Remaining.Start);
            this.responseCompletionSource!.TrySetResult(
                CreateResponse<AcknowledgeMessage, AcknowledgeResponse>(in message));
        }
    }

    public async ValueTask DisposeAsync()
    {
        await this.readerTask.ConfigureAwait(false);
        GC.SuppressFinalize(this);
    }
}
