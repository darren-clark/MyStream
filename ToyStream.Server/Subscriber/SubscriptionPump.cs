using ToyStream.Protocol;
using ToyStream.Protocol.Messages;
using ToyStream.Shared;

namespace ToyStream.Server;

/// <summary>
/// Pushes a subscription's records as they arrive. Owns the whole write side of its connection: the
/// read loop never answers on a subscriber connection, so there is exactly one writer and no lock.
/// </summary>
/// <remarks>
/// Per-connection state behind a reference, because the handler holding it is a struct and gets
/// value-copied into the connection.
/// </remarks>
public sealed class SubscriptionPump(ITopicService topicService)
{
    private readonly TaskCompletionSource<Subscription> subscribed =
        new(TaskCreationOptions.RunContinuationsAsynchronously);

    private ConnectionBase? connection;

    private readonly record struct Subscription(Utf8String Topic, SubscribeOffset StartOffset);

    /// <summary>
    /// Set after construction because the connection needs the handler that holds this pump, so one
    /// of the two has to be wired up second.
    /// </summary>
    public void Attach(ConnectionBase attached) => this.connection = attached;

    /// <summary>Called from the read loop, which is why nothing here blocks.</summary>
    public void Begin(Utf8String topic, SubscribeOffset startOffset) =>
        this.subscribed.TrySetResult(new Subscription(topic, startOffset));

    /// <summary>
    /// Runs until the subscription ends or the connection goes. Waits for the subscribe first, so
    /// starting this alongside the read loop costs nothing until a client asks for something.
    /// </summary>
    public async Task Run(CancellationToken cancellationToken)
    {
        var request = await this.subscribed.Task.WaitAsync(cancellationToken).ConfigureAwait(false);

        if (!topicService.TryGetTopic(request.Topic, out var topic))
        {
            await Lose(SubscriptionLostReason.UnknownTopic, cancellationToken).ConfigureAwait(false);
            return;
        }

        var partitions = new ITopicPartition[topic.PartitionCount];
        var offsets = new long[topic.PartitionCount];
        for (short i = 0; i < partitions.Length; i++)
        {
            if (!topic.TryGetPartition(i, out var partition))
            {
                await Lose(SubscriptionLostReason.UnknownTopic, cancellationToken).ConfigureAwait(false);
                return;
            }

            partitions[i] = partition;
            var range = partition.Describe();
            offsets[i] = request.StartOffset == SubscribeOffset.Latest ? range.EndOffset : range.StartOffset;
        }

        // An empty batch the moment the offsets are latched. Subscribe has no answer, so without
        // this a client cannot tell "established and idle" from "lost in flight" - and with Latest
        // it cannot know when its own publishes start counting.
        await PushBatch(partitions, offsets, cancellationToken).ConfigureAwait(false);

        while (!cancellationToken.IsCancellationRequested)
        {
            if (!HasRecords(partitions, offsets))
            {
                await WaitForAppend(partitions, offsets, cancellationToken).ConfigureAwait(false);
                continue;
            }

            await PushBatch(partitions, offsets, cancellationToken).ConfigureAwait(false);
        }
    }

    /// <summary>
    /// Everything ready goes out in one body. That is the whole batching story: one record when idle
    /// costs one message, a backlog costs one message too, and there is no size to configure.
    /// </summary>
    private async ValueTask PushBatch(ITopicPartition[] partitions, long[] offsets,
        CancellationToken cancellationToken)
    {
        var body = Connected().BeginOneWayBody(new PushRecordsMessage());
        try
        {
            var writer = new FetchedRecordWriter(body);
            for (short partition = 0; partition < partitions.Length; partition++)
            {
                while (partitions[partition].TryReadRecord(0, partition, offsets[partition], out var record))
                {
                    await writer.WriteRecord(record, cancellationToken).ConfigureAwait(false);
                    offsets[partition]++;
                }
            }

            await writer.CompleteAsync(cancellationToken).ConfigureAwait(false);
        }
        finally
        {
            Connected().EndOneWayBody();
        }
    }

    private static bool HasRecords(ITopicPartition[] partitions, long[] offsets)
    {
        for (var i = 0; i < partitions.Length; i++)
        {
            if (offsets[i] < partitions[i].Describe().EndOffset)
            {
                return true;
            }
        }

        return false;
    }

    /// <summary>
    /// Registers on every partition before re-checking, because an append landing between the two
    /// replaces the signal these waits hold and would otherwise only be seen by a later read.
    /// </summary>
    private static async Task WaitForAppend(ITopicPartition[] partitions, long[] offsets,
        CancellationToken cancellationToken)
    {
        using var linked = CancellationTokenSource.CreateLinkedTokenSource(cancellationToken);

        var waits = new Task[partitions.Length];
        for (var i = 0; i < partitions.Length; i++)
        {
            waits[i] = partitions[i].WaitForAppend(linked.Token);
        }

        // Re-checked only now: an append between the caller's look and here has already replaced the
        // signal these waits hold, so nothing but a second read could see it.
        if (!HasRecords(partitions, offsets))
        {
            await Task.WhenAny(waits).ConfigureAwait(false);
        }

        // Cancelled so the losers do not hold a registration on every partition for the life of the
        // subscription, then awaited so a partition that faulted is not left unobserved.
        await linked.CancelAsync().ConfigureAwait(false);
        try
        {
            await Task.WhenAll(waits).ConfigureAwait(false);
        }
        catch (OperationCanceledException)
        {
        }

        cancellationToken.ThrowIfCancellationRequested();
    }

    private ValueTask Lose(SubscriptionLostReason reason, CancellationToken cancellationToken) =>
        Connected().SendMessage(new SubscriptionLostMessage { Reason = reason }, cancellationToken);

    private ConnectionBase Connected() =>
        this.connection ?? throw new InvalidOperationException("The pump was run before it was attached.");
}
