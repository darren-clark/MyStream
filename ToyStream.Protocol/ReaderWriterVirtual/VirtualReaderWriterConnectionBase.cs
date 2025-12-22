namespace ToyStream.Protocol.ReaderWriterVirtual;

using System.Diagnostics;
using System.IO.Pipelines;
using System.Runtime.CompilerServices;
using ToyStream.Protocol.Messages;

public abstract class VirtualReaderWriterConnectionBase: IToyProtocolHandler, IAsyncDisposable
{
    private readonly Task readerTask;
    private readonly CancellationToken cancellationToken;
    private readonly IToyProtocolWriter writer;
    private readonly ResponseCompletionSourceCache cachedCompletionSources;
    private readonly string? tag;
    
    private ResponseCompletionSource? responseCompletionSource;

    protected VirtualReaderWriterConnectionBase(IToyProtocolReader reader, IToyProtocolWriter writer, CancellationToken cancellationToken, string? tag = null)
    {
        this.writer = writer;
        this.cachedCompletionSources = new();
        this.cancellationToken = cancellationToken;
        this.readerTask = reader.Read(this, cancellationToken);
        this.tag = tag;
    }

    protected Task<TResponse> SendMessage<TMessage, TResponseMessage, TResponse>(TMessage message, CancellationToken sendCancellationToken)
        where TMessage : struct, IRequestMessage<TMessage, TResponseMessage>, allows ref struct
        where TResponseMessage : struct, IResponseMessage<TResponseMessage>, allows ref struct
        where TResponse : class, IResponse<TResponseMessage, TResponse>
    {
        var completionSource = this.cachedCompletionSources.Get<TResponse>();
        this.responseCompletionSource = completionSource;
        
        return SendAndWait(
            this.writer.Write(message, sendCancellationToken), 
            completionSource, 
            sendCancellationToken.Register(static c => ((ResponseCompletionSource<TResponse>) c!).Cancel(), completionSource));

        async Task<TResponse> SendAndWait(ValueTask writerTask, ResponseCompletionSource<TResponse> responseCompletion, CancellationTokenRegistration cancellation)
        {
            await writerTask.ConfigureAwait(false);
            var result = await responseCompletion;
            await cancellation.DisposeAsync();
            return result;
        }
    }

    protected ValueTask SendResponse<T>(T response)
        where T : struct, IMessage<T>, allows ref struct
    {
        return this.writer.Write(response, this.cancellationToken);
    }


    ValueTask IToyProtocolHandler.HandleCreateTopicMessage(ref readonly CreateTopicMessage message)
        => this.HandleCreateTopicMessage(in message);

    protected virtual ValueTask HandleCreateTopicMessage(ref readonly CreateTopicMessage message) =>
        throw new NotImplementedException();

   
    ValueTask IToyProtocolHandler.HandleResponseMessage<TResponseMessage, TResponse>(ref readonly TResponseMessage message)
    {
        Debug.Assert(this.responseCompletionSource != null);
        this.responseCompletionSource.TrySetResult(TResponse.CreateResponse(in message));
        return default;
    }

    async ValueTask IAsyncDisposable.DisposeAsync()
    {
        await this.readerTask.ConfigureAwait(false);
        await this.DisposeAsync().ConfigureAwait(false);
    }

    protected virtual ValueTask DisposeAsync() => default;
    
    private abstract class ResponseCompletionSource: INotifyCompletion
    {
        private enum  State: byte
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
            var localState = Interlocked.CompareExchange(ref this.state, (byte) State.RegisteringCallback, (byte) State.Pending);
            // We won the race, the task is not yet completed
            if (localState == (byte)State.Pending)
            { 
                // If we get here then Complete() will wait until we set the callback.
                this.onCompletion = completion;
                Volatile.Write(ref this.state, (byte)State.CallbackRegistered);
            } 
            else if (this.state == (byte) State.Completed)
            {
                completion();
            }
            else
            {
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
                    Volatile.Write(ref this.state, (byte) State.Completed);
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
            Volatile.Write(ref this.state, (byte) State.Completed);
            this.cancelled = true;
        }


        public bool IsCompleted => this.state == (byte)State.Completed;
        
        public virtual void Reset()
        {
            this.onCompletion = null;   
            this.state = (byte)State.Pending;
            this.cancelled = false;
        }

        protected T GetResult<T>(T? result) where T: class
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

            return (ResponseCompletionSource<T>) source;
        }
    }
}

public abstract class StreamVirtualReaderWriterConnectionBase(Stream stream, CancellationToken cancellationToken) : VirtualReaderWriterConnectionBase(
    new ToyProtocolPipeReader(PipeReader.Create(stream)),
    new ToyProtocolPipeWriter(PipeWriter.Create(stream)),
    cancellationToken)
{
    protected override async ValueTask DisposeAsync()
    {
        await base.DisposeAsync();
        await stream.DisposeAsync();
    }
}