using System.Diagnostics.CodeAnalysis;

namespace ToyStream.Protocol.Dispatch;

using System.Buffers;
using System.IO.Pipelines;
using System.Runtime.CompilerServices;
using System.Runtime.InteropServices;

public delegate ValueTask
    AsyncDirectMessageHandler<TRequest, TResponse>(scoped in TRequest message, scoped in ResponseWriter<TResponse> responseWriter, CancellationToken cancellationToken)
    where TRequest : struct, IRequestMessage<TRequest, TResponse>, allows ref struct
    where TResponse : struct, IResponseMessage<TResponse>, allows ref struct;

public delegate void 
    DirectMessageHandler<TRequest, TResponse>(scoped in TRequest message, scoped in ResponseWriter<TResponse> responseWriter)
    where TRequest : struct, IRequestMessage<TRequest, TResponse>, allows ref struct
    where TResponse : struct, IResponseMessage<TResponse>, allows ref struct;

/// <summary>
/// Dispatch variant: an abstract method on the connection, overridden by the derived type.
/// Standalone by design - sharing a base with the other variants would make every arm pay for
/// whatever the base did, which is what makes bolt-on comparisons unfair.
/// </summary>
public abstract class MyDirectConnectionBase : IAsyncDisposable
{
    private readonly PipeReader reader;
    private readonly PipeWriter writer;
    private readonly ResponseCompletionSourceCache cachedCompletionSources = new();

    private Task? readerTask;
    private ResponseCompletionSource? responseCompletionSource;
    private ReadOnlySequence<byte> buffer =  ReadOnlySequence<byte>.Empty;

    protected MyDirectConnectionBase(PipeReader reader, PipeWriter writer)
    {
        this.reader = reader;
        this.writer = writer;
    }

    public async Task Start(CancellationToken cancellationToken)
    {
        await Task.Yield();
        this.readerTask = this.Read(cancellationToken);
        try
        {
            await this.readerTask;
        }
        catch(OperationCanceledException)
        {
        }
    }

    protected abstract Task Read(CancellationToken cancellationToken);

    protected ValueTask<TResponse> SendMessage<TRequestMessage, TResponseMessage, TResponse>(
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

    private MessageType TakeMessageType()
    {
        var messageBuffer = buffer.Slice(0, sizeof(MessageType));
        var messageType = MemoryMarshal.Read<MessageType>(messageBuffer.FirstSpan);
        buffer  = buffer.Slice(messageBuffer.End);
        return messageType;
    }
    
    protected bool TryGetMessageType(out MessageType messageType)
    {
        if (buffer.Length >= sizeof(MessageType))
        {
            messageType = TakeMessageType();
            return true;
        }

        messageType = default;
        return false;
    }
    
    protected ValueTask<MessageType> ReadMessageType(CancellationToken cancellationToken)
    {
        var readTask = this.reader.ReadAtLeastAsync(sizeof(MessageType), cancellationToken);
        if(readTask.IsCompletedSuccessfully)
        {
            buffer = readTask.Result.Buffer;
            return ValueTask.FromResult(TakeMessageType());
        }

        return SlowRead(readTask);

            
        async ValueTask<MessageType> SlowRead(ValueTask<ReadResult> readTask)
        {
            buffer = (await  readTask).Buffer;
            return TakeMessageType();
        }
    }

    /// <summary>Non-async so a flush that completes inline costs no state machine.</summary>
    private ValueTask Flush(CancellationToken cancellationToken)
    {
        var flushTask = this.writer.FlushAsync(cancellationToken);
        if (flushTask.IsCompletedSuccessfully)
        {
            flushTask.GetAwaiter().GetResult();
            return default;
        }

        return AwaitFlush(flushTask);

        [AsyncMethodBuilder(typeof(PoolingAsyncValueTaskMethodBuilder))]
        static async ValueTask AwaitFlush(ValueTask<FlushResult> pending)
            => await pending.ConfigureAwait(false);
    }    
    
    [DoesNotReturn]
    protected ValueTask ThrowUnexpected(MessageType messageType) => throw new InvalidOperationException($"Unexpected message type: {messageType}");
    
    protected ValueTask DispatchRequest<TRequestMessage, TResponseMessage>(DirectMessageHandler<TRequestMessage, TResponseMessage> handler, CancellationToken cancellationToken) 
        where TRequestMessage : struct, IRequestMessage<TRequestMessage, TResponseMessage>, allows ref struct
        where TResponseMessage : struct, IResponseMessage<TResponseMessage>, allows ref struct
    {
        var primitiveReader = new PrimitiveReader(buffer);
        if (!TRequestMessage.TryRead(ref primitiveReader, out TRequestMessage message, out int bytesToRead))
        {
            return DispatchRequestSlow(handler, bytesToRead, cancellationToken);
        }

        buffer = primitiveReader.Remaining;

        var responseWriter = new ResponseWriter<TResponseMessage>(this.writer);
        handler.Invoke(message, responseWriter);
        this.reader.AdvanceTo(buffer.Start);
        return this.Flush(cancellationToken);
    }

    [AsyncMethodBuilder(typeof(PoolingAsyncValueTaskMethodBuilder))]
    private async ValueTask DispatchRequestSlow<TRequestMessage, TResponseMessage>(DirectMessageHandler<TRequestMessage, TResponseMessage> handler, int bytesToRead, CancellationToken cancellationToken) 
        where TRequestMessage : struct, IRequestMessage<TRequestMessage, TResponseMessage>, allows ref struct
        where TResponseMessage : struct, IResponseMessage<TResponseMessage>, allows ref struct
    {
        while (true)
        {
            this.reader.AdvanceTo(buffer.Start, buffer.End);
            var readResult = await this.reader.ReadAtLeastAsync(bytesToRead, cancellationToken).ConfigureAwait(false);
            buffer = readResult.Buffer;

            var primitiveReader = new PrimitiveReader(buffer);
            if (!TRequestMessage.TryRead(ref primitiveReader, out TRequestMessage message, out bytesToRead))
            {
                continue;
            }

            buffer = primitiveReader.Remaining;

            var responseWriter = new ResponseWriter<TResponseMessage>(this.writer);
            handler.Invoke(message, responseWriter);
            this.reader.AdvanceTo(buffer.Start);
            await this.writer.FlushAsync(cancellationToken).ConfigureAwait(false);
            return;
        }
    }    

    protected ValueTask DispatchRequest<TRequestMessage, TResponseMessage>(AsyncDirectMessageHandler<TRequestMessage, TResponseMessage> handler, CancellationToken cancellationToken) 
        where TRequestMessage : struct, IRequestMessage<TRequestMessage, TResponseMessage>, allows ref struct
        where TResponseMessage : struct, IResponseMessage<TResponseMessage>, allows ref struct
    {
        var primitiveReader = new PrimitiveReader(buffer);
        if (!TRequestMessage.TryRead(ref primitiveReader, out TRequestMessage message, out int bytesToRead))
        {
            return DispatchRequestSlow(handler, bytesToRead, cancellationToken);
        }

        buffer = primitiveReader.Remaining;

        var responseWriter = new ResponseWriter<TResponseMessage>(this.writer);

        // The handler runs synchronously up to its first await, which is the only window in which
        // it may touch the message, so advancing afterwards is safe.
        var handlerTask = handler.Invoke(message, responseWriter, cancellationToken);
        this.reader.AdvanceTo(buffer.Start);

        if (!handlerTask.IsCompletedSuccessfully)
        {
            return AwaitHandlerThenFlush(handlerTask, cancellationToken);
        }

        handlerTask.GetAwaiter().GetResult();
        return this.Flush(cancellationToken);
    }

    [AsyncMethodBuilder(typeof(PoolingAsyncValueTaskMethodBuilder))]
    private async ValueTask AwaitHandlerThenFlush(ValueTask handlerTask, CancellationToken cancellationToken)
    {
        await handlerTask.ConfigureAwait(false);
        await this.writer.FlushAsync(cancellationToken).ConfigureAwait(false);
    }

    [AsyncMethodBuilder(typeof(PoolingAsyncValueTaskMethodBuilder))]
    private async ValueTask DispatchRequestSlow<TRequestMessage, TResponseMessage>(AsyncDirectMessageHandler<TRequestMessage, TResponseMessage> handler, int bytesToRead, CancellationToken cancellationToken) 
        where TRequestMessage : struct, IRequestMessage<TRequestMessage, TResponseMessage>, allows ref struct
        where TResponseMessage : struct, IResponseMessage<TResponseMessage>, allows ref struct
    {
        while (true)
        {
            this.reader.AdvanceTo(buffer.Start, buffer.End);
            var readResult = await this.reader.ReadAtLeastAsync(bytesToRead, cancellationToken).ConfigureAwait(false);
            buffer = readResult.Buffer;

            var primitiveReader = new PrimitiveReader(buffer);
            if (!TRequestMessage.TryRead(ref primitiveReader, out TRequestMessage message, out bytesToRead))
            {
                continue;
            }

            buffer = primitiveReader.Remaining;

            var responseWriter = new ResponseWriter<TResponseMessage>(this.writer);
            var handlerTask = handler.Invoke(message, responseWriter, cancellationToken);
            this.reader.AdvanceTo(buffer.Start);

            await handlerTask.ConfigureAwait(false);
            await this.writer.FlushAsync(cancellationToken).ConfigureAwait(false);
            return;
        }
    }    

    protected ValueTask HandleResponse<TResponseMessage, TResponse>(CancellationToken cancellationToken)
        where TResponseMessage : struct, IResponseMessage<TResponseMessage>, allows ref struct
        where TResponse: class, IResponse<TResponseMessage, TResponse>, new()
    {
        var primitiveReader = new PrimitiveReader(buffer);
        if (!TResponseMessage.TryRead(ref primitiveReader, out TResponseMessage message, out var bytesToRead))
        {
            return HandleResponseSlow<TResponseMessage, TResponse>(bytesToRead, cancellationToken);
        }

        buffer = primitiveReader.Remaining;
        this.responseCompletionSource!.TrySetResult(TResponse.CreateResponse(in message));
        this.reader.AdvanceTo(buffer.Start);
        return default;
    }

    [AsyncMethodBuilder(typeof(PoolingAsyncValueTaskMethodBuilder))]
    private async ValueTask HandleResponseSlow<TResponseMessage, TResponse>(int bytesToRead, CancellationToken cancellationToken)
        where TResponseMessage : struct, IResponseMessage<TResponseMessage>, allows ref struct
        where TResponse: class, IResponse<TResponseMessage, TResponse>, new()
    {
        while (true)
        {
            this.reader.AdvanceTo(buffer.Start, buffer.End);
            var readResult = await this.reader.ReadAtLeastAsync(bytesToRead, cancellationToken).ConfigureAwait(false);
            buffer = readResult.Buffer;

            var primitiveReader = new PrimitiveReader(buffer);
            if (!TResponseMessage.TryRead(ref primitiveReader, out TResponseMessage message, out bytesToRead))
            {
                continue;
            }

            buffer = primitiveReader.Remaining;
            this.responseCompletionSource!.TrySetResult(TResponse.CreateResponse(in message));
            this.reader.AdvanceTo(buffer.Start);
            return;
        }
    }
    
    public async ValueTask DisposeAsync()
    {
        if (this.readerTask != null)
        {
            await this.readerTask.ConfigureAwait(false);
        }
        GC.SuppressFinalize(this);
    }
}