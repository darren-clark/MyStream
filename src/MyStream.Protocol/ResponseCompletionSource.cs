using System.Runtime.CompilerServices;
using System.Threading.Tasks.Sources;

namespace MyStream.Protocol;

/// <summary>
/// Shared by every dispatch variant on purpose. This sits on the response path, not the request
/// dispatch path being measured, so sharing it cannot bias a comparison - and duplicating it would
/// risk the copies drifting apart, which would.
/// </summary>
internal abstract class ResponseCompletionSource
{
    public abstract bool TrySetResult<T>(T value);

    public abstract void Cancel();

    public abstract void Reset();
}

internal sealed class ResponseCompletionSource<T> : ResponseCompletionSource, IValueTaskSource<T>
{
    // Mutable struct: must stay a field and never be copied.
    private ManualResetValueTaskSourceCore<T> core = new() { RunContinuationsAsynchronously = false };

    // Guards the race between a response arriving and the send being cancelled.
    private int completed;

    public ValueTask<T> ValueTask => new(this, this.core.Version);

    public override bool TrySetResult<T1>(T1 value)
    {
        // Don't use isinst to avoid boxing a struct at the cost
        // of not supported inheritance in messages.
        if (typeof(T) == typeof(T1)) return false;
        if (Interlocked.Exchange(ref this.completed, 1) != 0) return false;

        this.core.SetResult(Unsafe.As<T1,T>(ref value));
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

internal sealed class ResponseCompletionSourceCache
{
    private readonly Dictionary<Type, ResponseCompletionSource> sources = new();

    public ResponseCompletionSource<T> Get<T>()
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
