namespace ToyStream.Client;

using System.Diagnostics;
using System.Diagnostics.CodeAnalysis;
using System.IO.Pipelines;
using System.Threading.Channels;
using System.Threading.Tasks.Sources;
using ToyStream.Protocol;
using ToyStream.Protocol.Messages;

public sealed class SubscriptionLostException(SubscriptionLostReason reason)
    : InvalidOperationException($"The subscription ended: {reason}.")
{
    public SubscriptionLostReason Reason { get; } = reason;
}

/// <summary>
/// Hands pushed batches from the read loop to whoever is reading them. A batch is a view over the
/// connection's buffer, so the read loop must stay inside the handler until the reader is done with
/// it - the same bargain <see cref="IStreamingResponse.BodyDrained"/> makes for a fetch.
/// </summary>
/// <remarks>
/// There is never more than one batch in flight - the channel holds one and the deliverer parks
/// until it is drained - so the reader and the signal that releases it are single instances reset
/// per batch rather than anything pooled.
/// </remarks>
internal sealed class SubscriptionStream(Func<CancellationToken, ValueTask>? batchDrained = null)
{
    /// <summary>
    /// One at a time: the deliverer waits on <see cref="drained"/> anyway, so the bound only makes
    /// the handoff explicit. Carries the reader so a reader with no batch has nothing to read from.
    /// </summary>
    private readonly Channel<FetchedRecordReader> batches = Channel.CreateBounded<FetchedRecordReader>(1);

    private readonly Gate drained = new();

    private FetchedRecordReader? reader;
    private FetchedRecordReader? current;

    public FetchedRecordReader Records => this.current ?? ThrowNoCurrentRecord();

    /// <summary>Called from the read loop, and returns only once the reader has finished the batch.</summary>
    public async ValueTask Deliver(PipeReader body, CancellationToken cancellationToken)
    {
        var records = this.reader ??= new FetchedRecordReader(body);
        records.Reset(body);

        // Armed before the batch is visible, so the reader cannot release a gate that is still set
        // from the batch before it.
        this.drained.Arm();

        await this.batches.Writer.WriteAsync(records, cancellationToken).ConfigureAwait(false);
        await this.drained.Wait(cancellationToken).ConfigureAwait(false);
    }

    /// <summary>Ends the stream, so a reader waiting for records is told rather than left waiting.</summary>
    public void Lose(SubscriptionLostReason reason) =>
        this.batches.Writer.TryComplete(new SubscriptionLostException(reason));

    /// <summary>The connection going away ends it too, which is the only other way this stops.</summary>
    public void Close(Exception? exception = null) => this.batches.Writer.TryComplete(exception);

    public async ValueTask<bool> MoveNextAsync(CancellationToken cancellationToken)
    {
        while (true)
        {
            if (this.current is { } records)
            {
                if (await records.MoveNextAsync(cancellationToken).ConfigureAwait(false))
                {
                    return true;
                }

                // Releases the read loop, which has been parked inside the handler all this time.
                this.current = null;
                this.drained.Release();

                // The one point where the caller has finished everything delivered so far, so it is
                // where a checkpoint is both cheap and exactly true.
                if (batchDrained is { } drain)
                {
                    await drain(cancellationToken).ConfigureAwait(false);
                }
            }

            try
            {
                this.current = await this.batches.Reader.ReadAsync(cancellationToken).ConfigureAwait(false);
            }
            catch (ChannelClosedException closed) when (closed.InnerException is not null)
            {
                throw closed.InnerException;
            }
            catch (ChannelClosedException)
            {
                return false;
            }
        }
    }

    /// <summary>Lets go of the batch in hand so the read loop is never left parked.</summary>
    public void Release()
    {
        this.current = null;
        this.drained.Release();
    }

    [DoesNotReturn]
    [StackTraceHidden]
    private static FetchedRecordReader ThrowNoCurrentRecord() =>
        throw new InvalidOperationException("There is no record to read; call MoveNextAsync first.");

    /// <summary>
    /// The deliverer's half of the handoff, reset per batch instead of allocated. Continuations run
    /// asynchronously because the thread that releases it is the reader's, and the read loop must
    /// not carry on from there.
    /// </summary>
    private sealed class Gate : IValueTaskSource
    {
        // Mutable struct: must stay a field and never be copied.
        private ManualResetValueTaskSourceCore<bool> core = new() { RunContinuationsAsynchronously = true };

        private CancellationTokenRegistration registration;
        private int completed = 1;

        /// <summary>Readied for a batch. Nothing may release it between batches.</summary>
        public void Arm()
        {
            this.core.Reset();
            Volatile.Write(ref this.completed, 0);
        }

        public ValueTask Wait(CancellationToken cancellationToken)
        {
            this.registration = cancellationToken.UnsafeRegister(
                static state => ((Gate)state!).Cancel(), this);

            return new ValueTask(this, this.core.Version);
        }

        /// <summary>Tolerates being called twice: a disposing reader releases whatever is in hand.</summary>
        public void Release()
        {
            if (Interlocked.Exchange(ref this.completed, 1) == 0)
            {
                this.core.SetResult(true);
            }
        }

        void IValueTaskSource.GetResult(short token)
        {
            this.registration.Dispose();
            this.registration = default;
            this.core.GetResult(token);
        }

        ValueTaskSourceStatus IValueTaskSource.GetStatus(short token) => this.core.GetStatus(token);

        void IValueTaskSource.OnCompleted(Action<object?> continuation, object? state, short token,
            ValueTaskSourceOnCompletedFlags flags) => this.core.OnCompleted(continuation, state, token, flags);

        private void Cancel()
        {
            if (Interlocked.Exchange(ref this.completed, 1) == 0)
            {
                this.core.SetException(new OperationCanceledException());
            }
        }
    }
}
