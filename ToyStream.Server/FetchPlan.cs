using System.Buffers;
using ToyStream.Protocol;
using ToyStream.Protocol.Messages;
using ToyStream.Protocol.Responses;
using ToyStream.Shared;

namespace ToyStream.Server;

/// <summary>
/// Everything a fetch needs once the request's views are gone: resolved topics and partitions, and
/// the entries copied out. Doubles as the cursor the response pulls records through, so nothing is
/// held beyond the record currently being written.
/// </summary>
internal sealed class FetchPlan
{
    private Utf8String[] names = [];
    private ITopicPartition[] partitions = [];
    private FetchEntry[] entries = [];
    private int topicCount;
    private int entryCount;

    private int cursorEntry;
    private long cursorOffset;
    private int remainingBytes;
    private int written;

    public static FetchPlan Rent(int topicCount, int entryCount) =>
        new()
        {
            names = ArrayPool<Utf8String>.Shared.Rent(Math.Max(topicCount, 1)),
            partitions = ArrayPool<ITopicPartition>.Shared.Rent(Math.Max(entryCount, 1)),
            entries = ArrayPool<FetchEntry>.Shared.Rent(Math.Max(entryCount, 1)),
            topicCount = topicCount,
            entryCount = entryCount
        };

    /// <summary>Validates the request and copies out everything that is a view over its buffer.</summary>
    public FetchResult Resolve(ITopicService topicService, FetchRequestMessage message)
    {
        for (var i = 0; i < this.topicCount; i++)
        {
            if (!topicService.TryGetTopic(message.Topics[i], out var topic))
            {
                return FetchResult.UnknownTopic;
            }

            this.names[i] = topic.Name;
        }

        var index = 0;
        foreach (var entry in message.Entries)
        {
            // The index comes off the wire, so it is checked before it reaches the table.
            if ((uint)entry.TopicIndex >= (uint)this.topicCount)
            {
                return FetchResult.UnknownTopic;
            }

            if (!topicService.TryGetTopic(this.names[entry.TopicIndex], out var topic) ||
                !topic.TryGetPartition(entry.PartitionId, out var partition))
            {
                return FetchResult.UnknownPartition;
            }

            var range = partition.Describe();
            if (entry.Offset < range.StartOffset || entry.Offset > range.EndOffset)
            {
                return FetchResult.OffsetOutOfRange;
            }

            this.partitions[index] = partition;
            this.entries[index] = entry;
            index++;
        }

        return FetchResult.Success;
    }

    /// <summary>Whether anything is ready, decided from the offsets rather than by reading.</summary>
    public bool HasRecords()
    {
        for (var i = 0; i < this.entryCount; i++)
        {
            if (this.entries[i].Offset < this.partitions[i].Describe().EndOffset)
            {
                return true;
            }
        }

        return false;
    }

    /// <summary>
    /// The next record to go out, read only as it is needed. The byte budget is checked before a
    /// read rather than after, so an outsized record is still returned on its own.
    /// </summary>
    private bool TryGetNext(out FetchedRecord record)
    {
        while (this.cursorEntry < this.entryCount &&
               this.written < ProtocolConstants.MaxFetchRecords &&
               this.remainingBytes > 0)
        {
            var entry = this.entries[this.cursorEntry];
            if (this.partitions[this.cursorEntry]
                .TryReadRecord(entry.TopicIndex, entry.PartitionId, this.cursorOffset, out record))
            {
                this.cursorOffset++;
                this.written++;
                this.remainingBytes -= (int)record.Body.Length + (int)record.Key.Length;
                return true;
            }

            this.cursorEntry++;
            this.cursorOffset = this.cursorEntry < this.entryCount ? this.entries[this.cursorEntry].Offset : 0;
        }

        record = default;
        return false;
    }

    /// <summary>Waits for any of the requested partitions, or until the caller's limit runs out.</summary>
    public async Task WaitForAppend(int maxWaitMilliseconds, CancellationToken cancellationToken)
    {
        using var timeout = CancellationTokenSource.CreateLinkedTokenSource(cancellationToken);
        timeout.CancelAfter(maxWaitMilliseconds);

        var waits = new Task[this.entryCount];
        for (var i = 0; i < this.entryCount; i++)
        {
            waits[i] = this.partitions[i].WaitForAppend(timeout.Token);
        }

        // Re-checked only now: an append between the caller's look and here has already replaced the
        // signal these waits hold, so nothing but a second read can see it.
        if (!HasRecords())
        {
            try
            {
                await Task.WhenAny(waits).ConfigureAwait(false);
            }
            catch (OperationCanceledException)
            {
            }
        }

        // Only the caller's own cancellation is worth reporting; the timeout is an empty answer.
        cancellationToken.ThrowIfCancellationRequested();
    }

    /// <summary>
    /// Header first, then the records as a streamed body. Separate from the streaming itself because
    /// a ref struct cannot be built inside an async method.
    /// </summary>
    public ValueTask WriteResponse(ResponseWriter<FetchResponseMessage> responseWriter, int maxBytes,
        CancellationToken cancellationToken)
    {
        this.cursorEntry = 0;
        this.cursorOffset = this.entryCount > 0 ? this.entries[0].Offset : 0;
        this.remainingBytes = maxBytes;
        this.written = 0;

        responseWriter.WriteResponse(new FetchResponseMessage
        {
            Result = FetchResult.Success,
            Topics = new TopicNames(this.names.AsSpan(0, this.topicCount))
        });

        return StreamRecords(responseWriter.BeginBody(), cancellationToken);
    }

    /// <summary>Pulled one at a time, so storage never materialises more than the record going out.</summary>
    private async ValueTask StreamRecords(MessageBodyWriter body, CancellationToken cancellationToken)
    {
        try
        {
            var writer = new FetchedRecordWriter(body);
            while (TryGetNext(out var record))
            {
                await writer.WriteRecord(record, cancellationToken).ConfigureAwait(false);
            }

            await writer.CompleteAsync(cancellationToken).ConfigureAwait(false);
        }
        finally
        {
            body.Complete();
        }
    }

    public void Return()
    {
        ArrayPool<Utf8String>.Shared.Return(this.names, clearArray: true);
        ArrayPool<ITopicPartition>.Shared.Return(this.partitions, clearArray: true);
        ArrayPool<FetchEntry>.Shared.Return(this.entries);
    }
}
