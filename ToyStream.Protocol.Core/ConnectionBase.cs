namespace ToyStream.Protocol;

using System.Diagnostics;
using System.Runtime.CompilerServices;
using Receive;
using Send;

public abstract class ConnectionBase: IToyProtocolHandler, IAsyncDisposable
{
    private readonly Task readerTask;
    private readonly CancellationToken cancellationToken;
    private readonly IToyProtocolWriter writer;
    private readonly ResponseCompletionSourceCache cachedCompletionSources = new();
    
    private ResponseCompletionSource? responseCompletionSource;
    
    protected ConnectionBase(IToyProtocolReader reader, IToyProtocolWriter writer, CancellationToken cancellationToken)
    {
        this.writer = writer;
        this.cancellationToken = cancellationToken;
        this.readerTask = reader.Read(this, cancellationToken);
    }

    protected Task<TResponse> SendMessage<TMessage, TResponse>(TMessage message, CancellationToken cancellationToken)
        where TMessage : struct, Protocol.Send.IMessage, allows ref struct
        where TResponse: struct, Protocol.Receive.IMessage
    {
        var completionSource = this.cachedCompletionSources.Get<TResponse>();
        cancellationToken.Register(completionSource.Cancel);
        this.responseCompletionSource = completionSource;
        return SendAndWait(this.writer.Write(message, cancellationToken), completionSource);

        static async Task<TResponse> SendAndWait(ValueTask writerTask, ResponseCompletionSource<TResponse> responseCompletion)
        {
            await writerTask.ConfigureAwait(false);
            return await responseCompletion;
        }
    }

    protected ValueTask SendResponse<T>(T response)
        where T : struct, Send.IMessage, allows ref struct
        => this.writer.Write(response, cancellationToken);

    ValueTask IToyProtocolHandler.HandleMessage<T>(ref readonly T message)
    {
        if (this.responseCompletionSource?.TrySetResult(message) ?? false)
        {
            return default;
        }

        return this.HandleMessageAsync(in message);
    }

    protected abstract ValueTask HandleMessageAsync<T>(ref readonly T message) where T : struct, Protocol.Receive.IMessage;
    
    protected abstract class ResponseCompletionSource: INotifyCompletion
    {
        private Action? onCompletion;
        private bool cancelled;

        public void OnCompleted(Action onCompletion)
        {
            this.onCompletion = onCompletion;
        }

        public void Complete()
        {
            Debug.Assert(this.onCompletion != null);
            this.onCompletion.Invoke();
            this.cancelled = false;
            this.onCompletion = null;
        }

        public void Cancel()
        {
            this.cancelled = true;
        }

        public abstract bool TrySetResult<T>(T result);

        public virtual void Reset()
        {
            Debug.Assert(this.onCompletion == null);
        }
        
        public abstract bool IsCompleted { get; }
        protected T GetResult<T>(T? result) where T: struct => this.cancelled ? throw new TaskCanceledException() : result ?? throw new InvalidOperationException();
    }

    /// <summary>
    /// Avoid creating a new TaskCompletionSource for every message.
    /// By having a bespoke awaiter for each message type.
    /// </summary>
    /// <typeparam name="T"></typeparam>
    protected class ResponseCompletionSource<T> : ResponseCompletionSource where T: struct, Protocol.Receive.IMessage
    {
        private T? result;
        
        public override bool TrySetResult<T1>(T1 result)
        {
            if (result is not T t) return false;
            
            this.result = t;
            this.Complete();
            return true;
        }

        public override void Reset()
        {
            base.Reset();
            this.result = null;
        }

        public ResponseCompletionSource<T> GetAwaiter() => this;

        public override bool IsCompleted => this.result is not null;

        public T GetResult() => this.GetResult<T>(this.result);
    }

    private class ResponseCompletionSourceCache
    {
        private readonly Dictionary<Type, ResponseCompletionSource> sources = new();
        
        public ResponseCompletionSource<T> Get<T>() where T : struct, Protocol.Receive.IMessage
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
    
    async ValueTask IAsyncDisposable.DisposeAsync()
    {
        await this.readerTask.ConfigureAwait(false);
        await this.DisposeAsync().ConfigureAwait(false);
    }

    protected virtual ValueTask DisposeAsync() => default;
}