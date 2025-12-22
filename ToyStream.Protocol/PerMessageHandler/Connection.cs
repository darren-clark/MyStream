namespace ToyStream.Protocol.PerMessageHandler;

using System.Buffers;
using System.Diagnostics;
using System.IO.Pipelines;
using System.Runtime.CompilerServices;
using System.Runtime.InteropServices;
using Messages;
using Shared;

/// <summary>
/// This is a virtual base class for connections
///
/// Where a derived class implements the handling.
/// </summary>
public abstract class Connection : IAsyncDisposable
{
    private readonly PipeReader reader;
    private readonly PipeWriter writer;
    private readonly CancellationTokenSource cts;
    private readonly Task readerTask;
    private readonly IMessageHandler messageHandler;
    private ResponseCompletionSource? responseCompletionSource;

    private readonly ResponseCompletionSourceCache cachedCompletionSources;

    public Connection(IDuplexPipe connection, IMessageHandler messageHandler)
    {
        this.reader = connection.Input;
        this.writer = connection.Output;
        this.cts = new();
        this.readerTask = this.Read();
        this.messageHandler = messageHandler;
        this.cachedCompletionSources = new();
    }

    private delegate ValueTask ProcessDelegate<TMessage>(ref readonly TMessage message)
        where TMessage : struct, IMessage<TMessage>, allows ref struct;

    private ValueTask HandleResponseMessage<TResponseMessage, TResponse>(in TResponseMessage message)
        where TResponseMessage : struct, IResponseMessage<TResponseMessage>, allows ref struct
        where TResponse : class, IResponse<TResponseMessage, TResponse>
    {
        Debug.Assert(this.responseCompletionSource != null);
        this.responseCompletionSource.TrySetResult(TResponse.CreateResponse(in message));
        return ValueTask.CompletedTask;
    }

    protected Task<TResponse> SendMessage<TMessage, TResponseMessage, TResponse>(TMessage message,
        CancellationToken sendCancellationToken)
        where TMessage : struct, IRequestMessage<TMessage, TResponseMessage>, allows ref struct
        where TResponseMessage : struct, IResponseMessage<TResponseMessage>, allows ref struct
        where TResponse : class, IResponse<TResponseMessage, TResponse>
    {
        var completionSource = this.cachedCompletionSources.Get<TResponse>();
        this.responseCompletionSource = completionSource;

        message.Write(PrimitiveWriter.Create(this.writer));
        return SendAndWait(
            this.writer.FlushAsync(sendCancellationToken),
            completionSource,
            sendCancellationToken.Register(static c => ((ResponseCompletionSource<TResponse>)c!).Cancel(),
                completionSource));


        static async Task<TResponse> SendAndWait(ValueTask<FlushResult> writerTask,
            ResponseCompletionSource<TResponse> responseCompletion, CancellationTokenRegistration cancellation)
        {
            await writerTask.ConfigureAwait(false);
            var result = await responseCompletion;
            await cancellation.DisposeAsync();
            return result;
        }
    }

    public async Task Read()
    {
        await Task.Yield();
        var buffer = ReadOnlySequence<byte>.Empty;
        var cancellationToken = this.cts.Token;
        try
        {
            while (!cancellationToken.IsCancellationRequested)
            {
                if (buffer.Length == 0)
                {
                    var result = await this.reader.ReadAsync(cancellationToken).ConfigureAwait(false);
                    buffer = result.Buffer;
                }

                var messageBuffer = buffer.Slice(0, 1);
                var messageTypeByte = MemoryMarshal.Read<byte>(messageBuffer.FirstSpan);
                buffer = buffer.Slice(messageBuffer.End);

                // disable CA2012(ValueTask must be immediately awaited)
                // iat is immediately awaited, just outside the switch.
#pragma warning disable CA2012
                // Is high bit set?
                // If so, it is a response message.
                if ((messageTypeByte & 0x40) != 0)
                {
                    var messageType = (MessageType)(messageTypeByte & 0x3F);
                    await (messageType switch
                    {
                        MessageType.CreateTopic => ReadAndProcess((ref readonly CreateTopicResponseMessage message) =>
                            this.HandleResponseMessage<CreateTopicResponseMessage, CreateTopicResponse>(in message)),
                        _ => ThrowHelpers.ThrowNotImplemented<ValueTask>()
                    });
                }
                else
                {
                    var messageType = (MessageType)messageTypeByte;
                    await (messageType switch
                    {
                        //TODO: See if this can be made pretty.
                        MessageType.CreateTopic => ReadAndProcess((ref readonly CreateTopicMessage message) =>
                            this.messageHandler.HandleCreateTopicMessage(in message,
                                (ref readonly CreateTopicResponseMessage response) =>
                                    response.Write(PrimitiveWriter.Create(this.writer)))),
                        _ => ThrowHelpers.ThrowNotImplemented<ValueTask>()
                    }).ConfigureAwait(false);
                }
#pragma warning restore CA2012
            }
        }
        catch (OperationCanceledException)
        {
        }

        return;


        async ValueTask ReadAndProcess<TMessage>(ProcessDelegate<TMessage> messageHandler)
            where TMessage : struct, IMessage<TMessage>, allows ref struct
        {
            var primitiveReader = new PrimitiveReader(buffer);

            // For some reason this doesn't compile if it's in the while condition.
            var read = TMessage.TryRead(ref primitiveReader, out var message, out var bytesToRead);
            while (!read)
            {
                this.reader.AdvanceTo(buffer.Start, buffer.End);
                var readResult = await this.reader
                    .ReadAtLeastAsync(bytesToRead, cancellationToken)
                    .ConfigureAwait(false);
                buffer = readResult.Buffer;
                primitiveReader = new PrimitiveReader(buffer);
                read = TMessage.TryRead(ref primitiveReader, out message, out bytesToRead);
            }

            buffer = primitiveReader.Remaining;
            this.reader.AdvanceTo(primitiveReader.Remaining.Start);
            await messageHandler(in message).ConfigureAwait(false);
        }
    }

    #region completion source stuff

    private abstract class ResponseCompletionSource : INotifyCompletion
    {
        private enum State : byte
        {
            Pending,
            RegisteringCallback,
            CallbackRegistered,
            Completed
        }

        private Action? onCompletion;

        private bool cancelled;

        // manage races between OnCompleted and Complete
        // only one should invoke the callback.
        private byte state;

        protected ResponseCompletionSource()
        {
        }

        public void OnCompleted(Action completion)
        {
            // IIF we're pending, set to registering.
            switch (Interlocked.CompareExchange(ref this.state, (byte)State.RegisteringCallback, (byte)State.Pending))
            {
                // We won the race, the task is not yet completed
                case (byte)State.Pending:
                    // If we get here then Complete() will wait until we set the callback.
                    this.onCompletion = completion;
                    Volatile.Write(ref this.state, (byte)State.CallbackRegistered);
                    break;
                case (byte)State.Completed:
                    completion();
                    break;
                default:
                    // We should not be able to get here.
                    throw new InvalidOperationException();
            }
        }

        public abstract bool TrySetResult<T>(T value);

        protected void TryComplete()
        {
            var localState = Interlocked.CompareExchange(ref this.state, (byte)State.Completed, (byte)State.Pending);
            switch (localState)
            {
                //We won, wait for OnCompleted to be called.
                case (byte)State.Pending:
                    // If we get here, we're expecting a call to OnCompleted, OR a check of IsCompleted and call to GetResult.
                    return;
                // We're in the middle of setting the callback, wait until that has settled.
                case (byte)State.RegisteringCallback:
                    SpinWait.SpinUntil(() => Volatile.Read(ref this.state) == (byte)State.CallbackRegistered);
                    goto case (byte)State.CallbackRegistered;
                case (byte)State.CallbackRegistered:
                    // Set completed before we invoke callback
                    Volatile.Write(ref this.state, (byte)State.Completed);
                    this.onCompletion!();
                    break;
            }
        }

        public void Cancel()
        {
            var localState = Interlocked.CompareExchange(ref this.state, (byte)State.Completed, (byte)State.Pending);

            if (localState == (byte)State.RegisteringCallback)
            {
                SpinWait.SpinUntil(() => Volatile.Read(ref this.state) == (byte)State.CallbackRegistered);
            }

            Volatile.Write(ref this.state, (byte)State.Completed);
            this.cancelled = true;
        }


        public bool IsCompleted => this.state == (byte)State.Completed;

        public virtual void Reset()
        {
            this.onCompletion = null;
            this.state = (byte)State.Pending;
            this.cancelled = false;
        }

        protected T GetResult<T>(T? result) where T : class
        {
            return this.cancelled ? throw new TaskCanceledException() : result ?? throw new InvalidOperationException();
        }
    }

    /// <summary>
    /// Avoid creating a new TaskCompletionSource for every message.
    /// By having a bespoke awaiter for each message type.
    /// </summary>
    /// <typeparam name="T"></typeparam>
    private sealed class ResponseCompletionSource<T> : ResponseCompletionSource where T : class, IResponse
    {
        private T? result;

        public ResponseCompletionSource()
        {
        }

        public override bool TrySetResult<T1>(T1 value)
        {
            Debug.Assert(this.result == null);
            if (value is not T t) return false;

            this.result = t;
            this.TryComplete();
            return true;
        }

        public override void Reset()
        {
            base.Reset();
            this.result = null;
        }

        public ResponseCompletionSource<T> GetAwaiter()
        {
            return this;
        }

        public T GetResult() => this.GetResult(this.result);
    }

    private class ResponseCompletionSourceCache
    {
        private readonly Dictionary<Type, ResponseCompletionSource> sources = new();


        public ResponseCompletionSource<T> Get<T>() where T : class, IResponse
        {
            if (this.sources.TryGetValue(typeof(T), out var source))
            {
                source.Reset();
                return (ResponseCompletionSource<T>)source;
            }

            source = new ResponseCompletionSource<T>();
            this.sources.Add(typeof(T), source);

            return (ResponseCompletionSource<T>)source;
        }
    }

    #endregion

    public async ValueTask DisposeAsync()
    {
        await CastAndDispose(this.cts);
        await CastAndDispose(this.readerTask);

        return;

        static async ValueTask CastAndDispose(IDisposable resource)
        {
            if (resource is IAsyncDisposable resourceAsyncDisposable)
                await resourceAsyncDisposable.DisposeAsync();
            else
                resource.Dispose();
        }
    }
}