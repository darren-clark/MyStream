namespace ToyStream.Protocol.Dispatch.Direct;

using System.Buffers;
using System.IO.Pipelines;
using System.Runtime.CompilerServices;
using Messages;
using Responses;

/// <summary>
/// Plumbing only - pipes, send/receive machinery, decode helpers. Deliberately does no message
/// routing: each role owns its own read loop, so its switch is exactly the set of messages that
/// role serves and a missing handler is a compile error rather than a runtime one.
/// </summary>
public abstract class DirectConnection : IAsyncDisposable
{
    private readonly ResponseCompletionSourceCache cachedCompletionSources = new();

    private ResponseCompletionSource? responseCompletionSource;
    private Task readerTask = Task.CompletedTask;

    protected DirectConnection(PipeReader reader, PipeWriter writer, CancellationToken cancellationToken)
    {
        this.Reader = reader;
        this.Writer = writer;
        this.CancellationToken = cancellationToken;
    }

    protected PipeReader Reader { get; }

    protected PipeWriter Writer { get; }

    protected CancellationToken CancellationToken { get; }

    /// <summary>Called by the derived constructor once its own fields are set.</summary>
    protected void StartReading() => this.readerTask = this.Read();

    protected abstract Task Read();

    protected ValueTask<TResponse> SendMessage<TRequestMessage, TResponseMessage, TResponse>(
        TRequestMessage requestMessage, CancellationToken sendCancellationToken)
        where TRequestMessage : struct, IRequestMessage<TRequestMessage, TResponseMessage>, allows ref struct
        where TResponseMessage : struct, IResponseMessage<TResponseMessage>, allows ref struct
        where TResponse : class, IResponse<TResponseMessage, TResponse>
    {
        var completionSource = this.cachedCompletionSources.Get<TResponse>();
        this.responseCompletionSource = completionSource;

        var primitiveWriter = new PrimitiveWriter<PipeWriter>(this.Writer);
        primitiveWriter.Write(TRequestMessage.Type);
        requestMessage.Write(primitiveWriter);

        return SendAndWait(
            this.Writer.FlushAsync(sendCancellationToken),
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

    /// <summary>Non-async so the common case where the flush completes inline costs no state machine.</summary>
    protected ValueTask Write<TMessage>(TMessage message)
        where TMessage : struct, IMessage<TMessage>, allows ref struct
    {
        PrimitiveWriter<PipeWriter> primitiveWriter = new(this.Writer);
        primitiveWriter.Write(TMessage.Type);
        message.Write(primitiveWriter);

        var flush = this.Writer.FlushAsync(this.CancellationToken);
        if (flush.IsCompletedSuccessfully)
        {
            flush.GetAwaiter().GetResult();
            return default;
        }

        return AwaitFlush(flush);

        [AsyncMethodBuilder(typeof(PoolingAsyncValueTaskMethodBuilder))]
        static async ValueTask AwaitFlush(ValueTask<FlushResult> flushTask)
            => await flushTask.ConfigureAwait(false);
    }

    /// <summary>
    /// Decode only - the caller's switch arm knows the concrete type, so it can call the handler
    /// directly where a generic helper could not.
    /// The reader must be the caller's local: a ref struct message decoded from a reader created
    /// here could not escape this method, since its lifetime would be tied to that local.
    /// </summary>
    internal static bool TryDecode<TMessage>(ref PrimitiveReader reader, out TMessage message)
        where TMessage : struct, IMessage<TMessage>, allows ref struct
        => TMessage.TryRead(ref reader, out message, out _);

    protected void CompleteResponse<TResponseMessage, TResponse>(ref readonly TResponseMessage message)
        where TResponseMessage : struct, IResponseMessage<TResponseMessage>, allows ref struct
        where TResponse : class, IResponse<TResponseMessage, TResponse>
        => this.responseCompletionSource!.TrySetResult(TResponse.CreateResponse(in message));

    /// <summary>The single async slow path, shared by every message type.</summary>
    protected async ValueTask<ReadOnlySequence<byte>> RefillAsync(ReadOnlySequence<byte> buffer)
    {
        this.Reader.AdvanceTo(buffer.Start, buffer.End);
        var result = await this.Reader.ReadAsync(this.CancellationToken).ConfigureAwait(false);
        return result.Buffer;
    }

    public async ValueTask DisposeAsync()
    {
        await this.readerTask.ConfigureAwait(false);
        GC.SuppressFinalize(this);
    }
}
