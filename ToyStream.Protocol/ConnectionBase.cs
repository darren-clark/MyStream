namespace ToyStream.Protocol;

using System.Diagnostics;
using System.IO.Pipelines;
using System.Runtime.CompilerServices;
using System.Text;
using System.Threading.Tasks.Sources;
using Messages;

public abstract class ConnectionBase: IToyProtocolHandler, IAsyncDisposable
{
    private readonly Task readerTask;
    private readonly CancellationToken cancellationToken;
    private readonly IToyProtocolWriter writer;
    private readonly ResponseCompletionSourceCache cachedCompletionSources;
    private readonly string? tag;
    
    private ResponseCompletionSource? responseCompletionSource;

    // Conditional so call sites don't evaluate their interpolated string arguments unless tracing is on.
    [Conditional("PROTOCOL_TRACE")]
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
        Console.WriteLine(sb.ToString());
    }
    
    protected ConnectionBase(IToyProtocolReader reader, IToyProtocolWriter writer, CancellationToken cancellationToken, string? tag = null)
    {
        this.writer = writer;
        this.cachedCompletionSources = new();
        this.cancellationToken = cancellationToken;
        this.readerTask = reader.Read(this, cancellationToken);
        this.tag = tag;
    }

    protected ValueTask<TResponse> SendMessage<TRequestMessage, TResponseMessage, TResponse>(TRequestMessage requestMessage, CancellationToken sendCancellationToken)
        where TRequestMessage : struct, IRequestMessage<TRequestMessage, TResponseMessage>, allows ref struct
        where TResponseMessage : struct, IResponseMessage<TResponseMessage>, allows ref struct
        where TResponse : class, IResponse<TResponseMessage, TResponse>
    {
        Log($"Sending {typeof(TRequestMessage).Name} message, current completion source is {this.responseCompletionSource}");
        var completionSource = this.cachedCompletionSources.Get<TResponse>();
        this.responseCompletionSource = completionSource;
        
        return SendAndWait(
            this.writer.Write(requestMessage, sendCancellationToken), 
            completionSource, 
            sendCancellationToken.Register(static c => ((ResponseCompletionSource<TResponse>) c!).Cancel(), completionSource));

        [AsyncMethodBuilder(typeof(PoolingAsyncValueTaskMethodBuilder<>))]
        static async ValueTask<TResponse> SendAndWait(ValueTask writerTask, ResponseCompletionSource<TResponse> responseCompletion, CancellationTokenRegistration cancellation)
        {
            await writerTask.ConfigureAwait(false);
            var result = await responseCompletion.ValueTask.ConfigureAwait(false);
            await cancellation.DisposeAsync();
            return result;
        }
    }

    protected ValueTask SendResponse<TResponseMessage>(TResponseMessage response)
        where TResponseMessage : struct, IResponseMessage<TResponseMessage>, allows ref struct
    {
        Log($"Sending response {typeof(TResponseMessage)}");
            
        return this.writer.Write(response, cancellationToken);
    }


    // Implemented implicitly so the interface map points straight at the most derived override.
    // An explicit implementation cannot be overridden, so it would cost a second dispatch.
    public virtual ValueTask HandleCreateTopicMessage(ref readonly CreateTopicMessage message) =>
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
    
    private abstract class ResponseCompletionSource
    {
        public abstract bool TrySetResult<T>(T value);

        public abstract void Cancel();

        public abstract void Reset();
    }

    /// <summary>
    /// Avoid creating a new TaskCompletionSource for every message.
    /// By having a bespoke awaiter for each message type.
    /// </summary>
    /// <typeparam name="T"></typeparam>
    private sealed class ResponseCompletionSource<T> : ResponseCompletionSource, IValueTaskSource<T> where T : class
    {
        // Mutable struct: must stay a field and never be copied.
        private ManualResetValueTaskSourceCore<T> core = new() { RunContinuationsAsynchronously = false };

        // Guards the race between a response arriving and the send being cancelled.
        private int completed;

        public ValueTask<T> ValueTask => new(this, this.core.Version);

        public override bool TrySetResult<T1>(T1 value)
        {
            if (value is not T typed) return false;
            if (Interlocked.Exchange(ref this.completed, 1) != 0) return false;

            this.core.SetResult(typed);
            return true;
        }

        public override void Cancel()
        {
            if (Interlocked.Exchange(ref this.completed, 1) != 0) return;

            this.core.SetException(new TaskCanceledException());
        }

        public override void Reset()
        {
            this.core.Reset();
            this.completed = 0;
        }

        T IValueTaskSource<T>.GetResult(short token) => this.core.GetResult(token);

        ValueTaskSourceStatus IValueTaskSource<T>.GetStatus(short token) => this.core.GetStatus(token);

        void IValueTaskSource<T>.OnCompleted(Action<object?> continuation, object? state, short token,
            ValueTaskSourceOnCompletedFlags flags) => this.core.OnCompleted(continuation, state, token, flags);
    }

    private class ResponseCompletionSourceCache
    {
        private readonly Dictionary<Type, ResponseCompletionSource> sources = new();

        public ResponseCompletionSource<T> Get<T>() where T : class
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