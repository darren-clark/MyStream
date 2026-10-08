using System.Buffers;
using System.IO.Pipelines;

namespace ToyStream.Protocol.Dispatch.Context;

public interface IConnectionContext
{
    /// <summary>
    /// On success advances <paramref name="buffer"/> past the message it consumed; on failure leaves it
    /// alone and sets <paramref name="bytesNeeded"/> to the shortfall beyond what the buffer already holds.
    /// </summary>
    DispatchResult TryParseAndDispatch(ref ReadOnlySequence<byte> buffer, ref int bytesNeeded,
        PipeWriter writer, CancellationToken cancellationToken);
}

public readonly struct DispatchResult
{
    private DispatchResult(bool wroteResponse, ValueTask task)
    {
        this.IsSuccess = true;
        this.WroteResponse = wroteResponse;
        this.Task = task;
    }

    public bool IsSuccess { get; }
    public bool WroteResponse { get; }
    public ValueTask Task { get; }

    public static DispatchResult Insufficient => default;

    public static DispatchResult Dispatched(ValueTask dispatched, bool wroteResponse = true) =>
        new(wroteResponse, dispatched);
}

/// <summary>
/// The whole read loop, shared by every role. Routing lives in TContext, so a client and a server
/// differ only by the context they close over - and because TContext is a struct the JIT specialises
/// this type per role, which devirtualises the dispatch call.
/// </summary>
public sealed class ContextConnection<TContext> where TContext : struct, IConnectionContext
{
    private readonly PipeReader reader;
    private readonly PipeWriter writer;
    private readonly TContext context;

    public ContextConnection(PipeReader reader, PipeWriter writer, TContext context)
    {
        this.reader = reader;
        this.writer = writer;
        this.context = context;
    }

    public async Task Start(CancellationToken cancellationToken)
    {
        await Task.Yield();
        var minimumRead = 1;

        try
        {
            while (!cancellationToken.IsCancellationRequested)
            {
                // ReadAtLeastAsync already completes synchronously when the data is buffered, so a
                // TryRead probe in front of it only adds a call and a second path through the loop.
                var result = await this.reader.ReadAtLeastAsync(minimumRead, cancellationToken)
                    .ConfigureAwait(false);

                var buffer = result.Buffer;
                // Our input is gone.
                if (result.IsCompleted && buffer.Length < minimumRead)
                {
                    break;
                }

                var wroteResponse = false;
                while (true)
                {
                    var bytesNeeded = 0;
                    var dispatched = this.context.TryParseAndDispatch(
                        ref buffer, ref bytesNeeded, this.writer, cancellationToken);

                    if (!dispatched.IsSuccess)
                    {
                        // ReadAtLeastAsync wants a total, and bytesNeeded is only the shortfall.
                        minimumRead = (int)buffer.Length + Math.Max(bytesNeeded, 1);
                        break;
                    }

                    wroteResponse |= dispatched.WroteResponse;
                    if (!dispatched.Task.IsCompleted)
                    {
                        await dispatched.Task.ConfigureAwait(false);
                    }
                }

                // Advance only once every handler has consumed its message, which is a view over this buffer.
                this.reader.AdvanceTo(buffer.Start, buffer.End);

                // One flush per read rather than per message, so a batch of pipelined requests pays once.
                if (wroteResponse)
                {
                    var flush = this.writer.FlushAsync(cancellationToken);
                    if (flush.IsCompletedSuccessfully)
                    {
                        flush.GetAwaiter().GetResult();
                    }
                    else
                    {
                        await flush.ConfigureAwait(false);
                    }
                }
            }
        }
        catch (OperationCanceledException)
        {
        }
    }
}