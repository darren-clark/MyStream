namespace ToyStream.Protocol.Dispatch;

using System.Buffers;
using System.IO.Pipelines;
using System.Runtime.CompilerServices;
using System.Runtime.InteropServices;
using Messages;
using Responses;

public sealed class MultipleImplementationConnection : IAsyncDisposable
{
    private readonly PipeReader reader;
    private readonly PipeWriter writer;
    private readonly CancellationToken cancellationToken;
    private readonly ResponseCompletionSourceCache cachedCompletionSources = new();
    private readonly Task readerTask;
    private readonly object handler;

    private ResponseCompletionSource? responseCompletionSource;

    internal MultipleImplementationConnection(PipeReader reader, PipeWriter writer, object handler, CancellationToken cancellationToken)
    {
        this.reader = reader;
        this.writer = writer;
        this.cancellationToken = cancellationToken;
        this.handler = handler;
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

        var primitiveWriter = new PrimitiveWriter<PipeWriter>(this.writer);
        primitiveWriter.Write(TRequestMessage.Type);
        requestMessage.Write(primitiveWriter);
        
        return SendAndWait(
            this.writer.FlushAsync(sendCancellationToken),
            completionSource,
            sendCancellationToken.Register(static c => ((ResponseCompletionSource<TResponse>)c!).Cancel(), completionSource));

        [AsyncMethodBuilder(typeof(PoolingAsyncValueTaskMethodBuilder<>))]
        static async ValueTask<TResponse> SendAndWait(ValueTask<FlushResult> writerTask,
            ResponseCompletionSource<TResponse> responseCompletion, CancellationTokenRegistration cancellation)
        {
            await writerTask.ConfigureAwait(false);
            var result = await responseCompletion.ValueTask.ConfigureAwait(false);
            await cancellation.DisposeAsync();
            return result;
        }
    }

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
                    MessageType.CreateTopic => DispatchRequest<CreateTopicMessage, AcknowledgeMessage>(),
                    MessageType.Acknowledge => HandleResponse<AcknowledgeMessage, AcknowledgeResponse>(),
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
        async ValueTask DispatchRequest<TRequestMessage, TResponseMessage>() where TRequestMessage : struct, IRequestMessage<TRequestMessage, TResponseMessage>, allows ref struct
        where TResponseMessage : struct, IResponseMessage<TResponseMessage>, allows ref struct
        {
            if (handler is not IMessageHandler<TRequestMessage, TResponseMessage> messageHandler)
                throw new InvalidOperationException($"Handler does not implement IMessageHandler<{typeof(TRequestMessage).Name}, {typeof(TResponseMessage).Name}>");
            
            var primitiveReader = new PrimitiveReader(buffer);
            var read = TRequestMessage.TryRead(ref primitiveReader, out TRequestMessage message, out int bytesToRead);
            while (!read)
            {
                this.reader.AdvanceTo(buffer.Start, buffer.End);
                var readResult = await this.reader.ReadAtLeastAsync(bytesToRead, this.cancellationToken).ConfigureAwait(false);
                buffer = readResult.Buffer;
                primitiveReader = new PrimitiveReader(buffer);
                read = TRequestMessage.TryRead(ref primitiveReader, out message, out bytesToRead);
            }
            buffer = primitiveReader.Remaining;
            
            var responseWriter = new ResponseWriter<TResponseMessage>(this.writer);
            await messageHandler.HandleRequest(ref message, responseWriter).ConfigureAwait(false);
            this.reader.AdvanceTo(buffer.Start);
            await this.writer.FlushAsync(cancellationToken).ConfigureAwait(false);
        }
        
        [AsyncMethodBuilder(typeof(PoolingAsyncValueTaskMethodBuilder))]
        async ValueTask HandleResponse<TResponseMessage, TResponse>()
            where TResponseMessage : struct, IResponseMessage<TResponseMessage>, allows ref struct
            where TResponse: class, IResponse<TResponseMessage, TResponse>, new()
        {
            var primitiveReader = new PrimitiveReader(buffer);
            var read = TResponseMessage.TryRead(ref primitiveReader, out TResponseMessage message, out var bytesToRead);

            while (!read)
            {
                this.reader.AdvanceTo(buffer.Start, buffer.End);
                var readResult = await this.reader.ReadAtLeastAsync(bytesToRead, this.cancellationToken).ConfigureAwait(false);
                buffer = readResult.Buffer;
                primitiveReader = new PrimitiveReader(buffer);
                read = TResponseMessage.TryRead(ref primitiveReader, out message, out bytesToRead);
            }

            buffer = primitiveReader.Remaining;
            this.responseCompletionSource!.TrySetResult(TResponse.CreateResponse(in message));
            this.reader.AdvanceTo(buffer.Start);
        }
    }

    public async ValueTask DisposeAsync()
    {
        await this.readerTask.ConfigureAwait(false);
        GC.SuppressFinalize(this);
    }
}