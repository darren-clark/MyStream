namespace ToyStream.Client;

using System.Diagnostics;
using System.Diagnostics.CodeAnalysis;
using System.IO.Pipelines;
using System.Threading.Channels;
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
internal sealed class SubscriptionStream
{
    /// <summary>
    /// One at a time: the deliverer waits on <see cref="Batch.Drained"/> anyway, so the bound only
    /// makes the handoff explicit.
    /// </summary>
    private readonly Channel<Batch> batches = Channel.CreateBounded<Batch>(1);

    private Batch? current;

    private sealed record Batch(FetchedRecordReader Records, TaskCompletionSource Drained);

    public FetchedRecordReader Records => Open().Records;

    /// <summary>Called from the read loop, and returns only once the reader has finished the batch.</summary>
    public async ValueTask Deliver(PipeReader body, CancellationToken cancellationToken)
    {
        var batch = new Batch(
            new FetchedRecordReader(body), new TaskCompletionSource(TaskCreationOptions.RunContinuationsAsynchronously));

        await this.batches.Writer.WriteAsync(batch, cancellationToken).ConfigureAwait(false);
        await batch.Drained.Task.WaitAsync(cancellationToken).ConfigureAwait(false);
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
            if (this.current is { } batch)
            {
                if (await batch.Records.MoveNextAsync(cancellationToken).ConfigureAwait(false))
                {
                    return true;
                }

                // Releases the read loop, which has been parked inside the handler all this time.
                this.current = null;
                batch.Drained.TrySetResult();
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
        if (this.current is { } batch)
        {
            this.current = null;
            batch.Drained.TrySetResult();
        }
    }

    private Batch Open() => this.current ?? ThrowNoCurrentRecord();

    [DoesNotReturn]
    [StackTraceHidden]
    private static Batch ThrowNoCurrentRecord() =>
        throw new InvalidOperationException("There is no record to read; call MoveNextAsync first.");
}
