namespace ToyStream.Protocol.Dispatch;

using System.Buffers;
using System.IO.Pipelines;
using System.Runtime.CompilerServices;
using System.Runtime.InteropServices;
using Messages;
using Responses;

public delegate ValueTask
    MessageHandler<TRequest, TResponse>(in TRequest message, in ResponseWriter<TResponse> responseWriter)
    where TRequest : struct, IRequestMessage<TRequest, TResponse>, allows ref struct
    where TResponse : struct, IResponseMessage<TResponse>, allows ref struct;

public class MapConnectionBuilder
{
    private readonly Dictionary<MessageType, object> handlers = new();

    public MapConnection Build(PipeReader reader, PipeWriter writer, CancellationToken cancellationToken)
    {
        return new  MapConnection(reader, writer, handlers, cancellationToken);
    }
    
    public void Add<TRequest, TResponse>(MessageHandler<TRequest, TResponse> handler)
        where TRequest : struct, IRequestMessage<TRequest, TResponse>, allows ref struct
        where TResponse : struct, IResponseMessage<TResponse>, allows ref struct
    {
        handlers[TRequest.Type] = handler;
    }
}

/// <summary>
/// Dispatch variant: an abstract method on the connection, overridden by the derived type.
/// Standalone by design - sharing a base with the other variants would make every arm pay for
/// whatever the base did, which is what makes bolt-on comparisons unfair.
/// </summary>
public sealed class MapConnection : IAsyncDisposable
{
    private readonly PipeReader reader;
    private readonly PipeWriter writer;
    private readonly CancellationToken cancellationToken;
    private readonly ResponseCompletionSourceCache cachedCompletionSources = new();
    private readonly Task readerTask;
    private readonly Dictionary<MessageType, object> handlers;

    private ResponseCompletionSource? responseCompletionSource;

    internal MapConnection(PipeReader reader, PipeWriter writer, Dictionary<MessageType, object> handlers, CancellationToken cancellationToken)
    {
        this.reader = reader;
        this.writer = writer;
        this.cancellationToken = cancellationToken;
        this.handlers = handlers;
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

    // private ValueTask Write<TMessage>(TMessage message, CancellationToken writeCancellationToken)
    //     where TMessage : struct, IMessage<TMessage>, allows ref struct
    // {
    //     PrimitiveWriter<PipeWriter> primitiveWriter = new(this.writer);
    //     primitiveWriter.Write(TMessage.Type);
    //     message.Write(primitiveWriter);
    //
    //     return Flush();
    //
    //     [AsyncMethodBuilder(typeof(PoolingAsyncValueTaskMethodBuilder))]
    //     async ValueTask Flush() => await this.writer.FlushAsync(writeCancellationToken).ConfigureAwait(false);
    // }

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
            await ((MessageHandler<TRequestMessage, TResponseMessage>)this.handlers[TRequestMessage.Type]).Invoke(in message, in responseWriter);
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