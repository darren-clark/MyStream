namespace ToyStream.Protocol;

using System.Diagnostics;
using System.IO.Pipelines;
using System.Runtime.CompilerServices;
using System.Text;
using Messages;

public abstract class ConnectionBase: IToyProtocolHandler, IAsyncDisposable
{
    private readonly Task readerTask;
    private readonly CancellationToken cancellationToken;
    private readonly IToyProtocolWriter writer;
    private readonly ResponseCompletionSourceCache cachedCompletionSources;
    private readonly string? tag;
    
    private ResponseCompletionSource? responseCompletionSource;

    protected void Log(string log)
    {
        StringBuilder sb = new();
        sb.Append($"[{DateTime.Now:HH:mm:ss.fffffff}]");
        if (tag != null)
        {
            sb.Append($"[{tag}]");
        }
        sb.Append($"[{Environment.CurrentManagedThreadId:D2}]");
        sb.Append($": {log}");
//        Console.WriteLine(sb.ToString());
    }
    
    protected ConnectionBase(IToyProtocolReader reader, IToyProtocolWriter writer, CancellationToken cancellationToken, string? tag = null)
    {
        this.writer = writer;
        this.cachedCompletionSources = new(this);
        this.cancellationToken = cancellationToken;
        this.readerTask = reader.Read(this, cancellationToken);
        this.tag = tag;
    }

    protected Task<TResponse> SendMessage<TRequestMessage, TResponseMessage, TResponse>(TRequestMessage requestMessage, CancellationToken sendCancellationToken)
        where TRequestMessage : struct, IRequestMessage<TRequestMessage, TResponseMessage, TResponse>, allows ref struct
        where TResponseMessage : struct, IResponseMessage<TResponseMessage, TResponse>, allows ref struct
        where TResponse : class
    {
        Log($"Sending {typeof(TRequestMessage).Name} message, current completion source is {this.responseCompletionSource}");
        var completionSource = this.cachedCompletionSources.Get<TResponse>();
        this.responseCompletionSource = completionSource;
        
        return SendAndWait(
            this.writer.Write(requestMessage, sendCancellationToken), 
            completionSource, 
            sendCancellationToken.Register(static c => ((ResponseCompletionSource<TResponse>) c!).Cancel(), completionSource));

        async Task<TResponse> SendAndWait(ValueTask writerTask, ResponseCompletionSource<TResponse> responseCompletion, CancellationTokenRegistration cancellation)
        {
            Log($"Sending message");
            await writerTask.ConfigureAwait(false);
            Log("Waiting response");
            var result = await responseCompletion;
            Log("Disposing cancellation registration");
            await cancellation.DisposeAsync();
            Log("Returning result");
            return result;
        }
    }

    protected ValueTask SendResponse<TResponseMessage>(TResponseMessage response)
        where TResponseMessage : struct, IResponseMessage<TResponseMessage>, allows ref struct
    {
        Log($"Sending response {typeof(TResponseMessage)}");
            
        return this.writer.Write(response, cancellationToken);
    }


    ValueTask IToyProtocolHandler.HandleCreateTopicMessage(ref readonly CreateTopicMessage message)
        => this.HandleCreateTopicMessage(in message);

    protected virtual ValueTask HandleCreateTopicMessage(ref readonly CreateTopicMessage message) =>
        throw new NotImplementedException();
    
    ValueTask IToyProtocolHandler.HandleResponseMessage<TResponseMessage, TResponse>(ref readonly TResponseMessage message)
    {
        Debug.Assert(this.responseCompletionSource != null);
        this.responseCompletionSource.TrySetResult(message.Response);
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
        
       
        protected ConnectionBase connection;

        protected ResponseCompletionSource(ConnectionBase connection)
        {
            this.connection = connection;
        }
        
        public void OnCompleted(Action completion)
        {
            // IIF we're pending, set to registering.
            var localState = Interlocked.CompareExchange(ref this.state, (byte) State.RegisteringCallback, (byte) State.Pending);
            this.connection.Log($"OnCompleted called in state {(State) localState}");
            // We won the race, the task is not yet completed
            if (localState == (byte)State.Pending)
            { 
                // If we get here then Complete() will wait until we set the callback.
                this.onCompletion = completion;
                Volatile.Write(ref this.state, (byte)State.CallbackRegistered);
            } 
            else if (state == (byte) State.Completed)
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
            this.connection.Log($"TryComplete called in state {(State) localState}");
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
            this.connection.Log($"Cancel called in state {(State) localState}");

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
            connection.Log($"Reset called, completion is {this.onCompletion}");
            this.onCompletion = null;   
            this.state = (byte)State.Pending;
            this.cancelled = false;
        }

        protected T GetResult<T>(T? result) where T: class
        {
            // lock (this.@lock)
            // {
                connection.Log($"GetResult called, completion is {this.onCompletion}, result is {result}");
                return this.cancelled ? throw new TaskCanceledException() : result ?? throw new InvalidOperationException();
            // }
        }
    }

    /// <summary>
    /// Avoid creating a new TaskCompletionSource for every message.
    /// By having a bespoke awaiter for each message type.
    /// </summary>
    /// <typeparam name="T"></typeparam>
    private sealed class ResponseCompletionSource<T> : ResponseCompletionSource where T : class
    {
        private T? result;

        public ResponseCompletionSource(ConnectionBase connection) : base(connection)
        {
        }

        public override bool TrySetResult<T1>(T1 value)
        {
            this.connection.Log($"TrySetResult called, value is {value}");
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
            this.connection.Log("GetAwaiter called");
            return this;
        }

        public T GetResult() => this.GetResult(this.result);
    }

    private class ResponseCompletionSourceCache(ConnectionBase connection)
    {
        private readonly Dictionary<Type, ResponseCompletionSource> sources = new();
    
        
        public ResponseCompletionSource<T> Get<T>() where T : class
        {
            if (this.sources.TryGetValue(typeof(T), out var source))
            {
                source.Reset();
                return (ResponseCompletionSource<T>)source;
            }
            
            source = new ResponseCompletionSource<T>(connection);
            this.sources.Add(typeof(T), source);

            return (ResponseCompletionSource<T>) source;
        }
    }
}

public abstract class StreamConnectionBase : ConnectionBase
{
    private readonly Stream stream;

    protected StreamConnectionBase(Stream stream, CancellationToken cancellationToken) : base(
        new ToyProtocolPipeReader(PipeReader.Create(stream)),
        new ToyProtocolPipeWriter(PipeWriter.Create(stream)),
            cancellationToken)
    {
        this.stream = stream;
    }

    protected override async ValueTask DisposeAsync()
    {
        await base.DisposeAsync();
        await this.stream.DisposeAsync();
    }
}