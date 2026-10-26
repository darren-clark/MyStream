using System.IO.Pipelines;
using ToyStream.Protocol.Messages;
using ToyStream.Shared;

namespace ToyStream.Protocol.Responses;

/// <summary>
/// A fetch's answer, read a record at a time. The records arrive as a stream rather than a parsed
/// message, so nothing bounds how large one may be and nothing is held but the record in hand.
/// </summary>
/// <remarks>
/// The connection is this caller's until it is disposed, which is what makes the body safe to read.
/// </remarks>
public sealed class FetchResponse : IStreamingResponse<FetchResponseMessage, FetchResponse>, IAsyncDisposable
{
    private readonly TaskCompletionSource drained = new(TaskCreationOptions.RunContinuationsAsynchronously);
    private readonly FetchedRecordReader records;
    private readonly Utf8String[] topics;

    private FetchResponse(FetchResult result, Utf8String[] topics, PipeReader body)
    {
        Result = result;
        this.topics = topics;
        this.records = new FetchedRecordReader(body);
    }

    public FetchResult Result { get; }

    /// <summary>Named once, so a record identifies its topic by index into this.</summary>
    public ReadOnlySpan<Utf8String> Topics => this.topics;

    public FetchedRecordHeader Current => this.records.Current;

    public ReadOnlyMemory<byte> Key => this.records.Key;

    ValueTask IStreamingResponse.BodyDrained => new(this.drained.Task);

    static FetchResponse IStreamingResponse<FetchResponseMessage, FetchResponse>.CreateResponse(
        ref readonly FetchResponseMessage header, PipeReader body)
    {
        // Copied out: the table is a view over the connection's buffer, which the body recycles.
        var topics = new Utf8String[header.Topics.Count];
        for (var i = 0; i < topics.Length; i++)
        {
            var name = header.Topics[i];
            var encoded = new byte[name.GetByteCount()];
            name.CopyTo(encoded);
            topics[i] = new Utf8String(encoded);
        }

        return new FetchResponse(header.Result, topics, body);
    }

    /// <summary>
    /// Advances to the next record, skipping the body of the current one if it was not read. False
    /// once the stream is exhausted, which is immediate for a failure.
    /// </summary>
    public ValueTask<bool> MoveNextAsync(CancellationToken cancellationToken = default) =>
        this.records.MoveNextAsync(cancellationToken);

    /// <summary>
    /// Streams the current record's body out. Flushed as it goes, so a record larger than the
    /// destination's buffer paces the sender rather than being held whole anywhere.
    /// </summary>
    public ValueTask CopyBodyTo(PipeWriter destination, CancellationToken cancellationToken = default) =>
        this.records.CopyBodyTo(destination, cancellationToken);

    /// <summary>
    /// Hands the connection back. Whatever is left of the stream is the connection's to drain, so
    /// stopping early costs a position rather than desynchronising anything.
    /// </summary>
    public ValueTask DisposeAsync()
    {
        this.drained.TrySetResult();
        return default;
    }
}
