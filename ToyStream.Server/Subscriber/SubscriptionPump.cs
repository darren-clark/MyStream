using ToyStream.Protocol;
using ToyStream.Protocol.Messages;
using ToyStream.Shared;

namespace ToyStream.Server;

/// <summary>
/// Pushes a subscription's records as they arrive. Owns the whole write side of its connection: the
/// read loop only ever reads on a subscriber connection, so there is exactly one writer and no lock.
/// </summary>
/// <remarks>
/// Per-connection state behind a reference, because the handler holding it is a struct and gets
/// value-copied into the connection.
/// </remarks>
public sealed class SubscriptionPump(
    ITopicService topicService,
    IConsumerGroupService consumerGroups,
    ConsumerGroupCoordinator coordinator)
{
    private readonly TaskCompletionSource<Subscription> subscribed =
        new(TaskCreationOptions.RunContinuationsAsynchronously);

    private ConnectionBase? connection;
    private GroupMember? member;
    private bool leaving;

    private readonly record struct Subscription(Utf8String Topic, Utf8String Group, Utf8String MemberId,
        SubscribeOffset StartOffset)
    {
        /// <summary>An anonymous subscriber keeps no checkpoint, so it has nothing to resume from.</summary>
        public bool IsGrouped => !Group.Memory.IsEmpty;
    }

    /// <summary>
    /// Set after construction because the connection needs the handler that holds this pump, so one
    /// of the two has to be wired up second.
    /// </summary>
    public void Attach(ConnectionBase attached) => this.connection = attached;

    /// <summary>Called from the read loop, which is why nothing here blocks.</summary>
    public void Begin(Utf8String topic, Utf8String group, Utf8String memberId, SubscribeOffset startOffset) =>
        this.subscribed.TrySetResult(new Subscription(topic, group, memberId, startOffset));

    /// <summary>
    /// Also called from the read loop, and onto a store the pump reads from another thread, so the
    /// store is what makes this safe rather than any ordering between the two.
    /// </summary>
    public void Commit(CommittedOffsets offsets)
    {
        // A commit before the subscribe, or from a subscriber with nowhere to put one, has no key.
        if (!this.subscribed.Task.IsCompletedSuccessfully)
        {
            return;
        }

        var request = this.subscribed.Task.Result;
        if (!request.IsGrouped)
        {
            return;
        }

        var seat = Volatile.Read(ref this.member);
        foreach (var offset in offsets)
        {
            // Checked against the live assignment: a straggler arriving after the partition moved on
            // would otherwise overwrite its new owner's progress.
            if (seat is null || seat.Owns(offset.PartitionId))
            {
                consumerGroups.Commit(request.Group, request.Topic, offset.PartitionId, offset.Offset);
            }
        }
    }

    /// <summary>The subscriber's answer to a revoke, which is what lets the handover finish.</summary>
    public void CompleteRevoke(int epoch, CommittedOffsets offsets) =>
        Volatile.Read(ref this.member)?.CompleteRevoke(epoch, offsets);

    /// <summary>
    /// Said it is going, so the group has nothing to wait for. Latched rather than acted on here
    /// because the seat is only given up once the pump has actually stopped.
    /// </summary>
    public void LeaveGroup() => Volatile.Write(ref this.leaving, true);

    /// <summary>
    /// Runs until the subscription ends or the connection goes. Waits for the subscribe first, so
    /// starting this alongside the read loop costs nothing until a client asks for something.
    /// </summary>
    public async Task Run(CancellationToken cancellationToken)
    {
        var request = await this.subscribed.Task.WaitAsync(cancellationToken).ConfigureAwait(false);

        if (request.StartOffset == SubscribeOffset.Committed && !request.IsGrouped)
        {
            await Lose(SubscriptionLostReason.CommittedOffsetRequiresGroup, cancellationToken).ConfigureAwait(false);
            return;
        }

        if (!topicService.TryGetTopic(request.Topic, out var topic))
        {
            await Lose(SubscriptionLostReason.UnknownTopic, cancellationToken).ConfigureAwait(false);
            return;
        }

        var partitions = new ITopicPartition[topic.PartitionCount];
        for (short i = 0; i < partitions.Length; i++)
        {
            if (!topic.TryGetPartition(i, out var partition))
            {
                await Lose(SubscriptionLostReason.UnknownTopic, cancellationToken).ConfigureAwait(false);
                return;
            }

            partitions[i] = partition;
        }

        var seat = request.IsGrouped
            ? coordinator.Join(request.Group, request.Topic, request.MemberId, (short)partitions.Length)
            : ConsumerGroupCoordinator.Alone((short)partitions.Length);

        Volatile.Write(ref this.member, seat);
        var attachment = seat.Attachment;
        try
        {
            await Pump(request, seat, partitions, cancellationToken).ConfigureAwait(false);
        }
        finally
        {
            // A subscriber that said it was going is gone; one that simply stopped answering keeps
            // its partitions for the linger, in case it is coming straight back.
            if (Volatile.Read(ref this.leaving))
            {
                seat.Leave(attachment);
            }
            else
            {
                seat.Depart(attachment);
            }
        }
    }

    private async Task Pump(Subscription request, GroupMember seat, ITopicPartition[] partitions,
        CancellationToken cancellationToken)
    {
        var owned = new bool[partitions.Length];
        var cursors = new long[partitions.Length];
        short[]? reconciled = null;
        var established = false;

        while (!cancellationToken.IsCancellationRequested)
        {
            // Taken before anything is read, so a change landing mid-pass completes this rather than
            // going unnoticed between the look and the wait.
            var changed = seat.Changed;

            while (seat.TryDequeueRevoke(out var revoke))
            {
                // Stops pushing them first, so the marker's place in the stream is what fixes the
                // final offset: everything for these partitions precedes it.
                foreach (var partition in revoke.Partitions)
                {
                    owned[partition] = false;
                }

                await SendRevoke(Connected(), revoke.Epoch, revoke.Partitions, cancellationToken)
                    .ConfigureAwait(false);

                _ = ExpireRevoke(seat, revoke.Epoch, coordinator.DrainTimeout, cancellationToken);
            }

            var assignment = seat.Assigned;
            if (!ReferenceEquals(assignment, reconciled))
            {
                Reconcile(request, assignment, owned, cursors, partitions);
                reconciled = assignment;
            }

            // An empty batch the moment the first assignment lands. Subscribe has no answer, so
            // without this a client cannot tell "established and idle" from "lost in flight".
            if (!established || HasRecords(partitions, owned, cursors))
            {
                established = true;
                await PushBatch(partitions, owned, cursors, cancellationToken).ConfigureAwait(false);
                continue;
            }

            await WaitForWork(partitions, owned, cursors, changed, cancellationToken).ConfigureAwait(false);
        }
    }

    /// <summary>
    /// Only an arriving partition gets a cursor resolved; one already held keeps the position it is
    /// reading from, so a rebalance elsewhere in the group never rewinds it.
    /// </summary>
    private void Reconcile(Subscription request, short[] assignment, bool[] owned, long[] cursors,
        ITopicPartition[] partitions)
    {
        Span<bool> next = stackalloc bool[owned.Length];
        foreach (var partition in assignment)
        {
            next[partition] = true;
        }

        for (short partition = 0; partition < owned.Length; partition++)
        {
            if (next[partition] && !owned[partition])
            {
                cursors[partition] = Resolve(request, partition, partitions[partition]);
            }

            owned[partition] = next[partition];
        }
    }

    /// <summary>
    /// A group's checkpoint wins wherever there is one, including on a handover - the start offset
    /// only decides where a group that has never committed begins.
    /// </summary>
    private long Resolve(Subscription request, short partition, ITopicPartition topicPartition)
    {
        var range = topicPartition.Describe();

        if (request.IsGrouped &&
            consumerGroups.TryGetCommittedOffset(request.Group, request.Topic, partition, out var committed))
        {
            // Clamped because a checkpoint older than the log has had its records evicted, and
            // resuming from the earliest surviving one beats refusing to resume at all.
            return Math.Clamp(committed, range.StartOffset, range.EndOffset);
        }

        return request.StartOffset == SubscribeOffset.Latest ? range.EndOffset : range.StartOffset;
    }

    private static async Task ExpireRevoke(GroupMember seat, int epoch, TimeSpan after,
        CancellationToken cancellationToken)
    {
        try
        {
            await Task.Delay(after, cancellationToken).ConfigureAwait(false);
        }
        catch (OperationCanceledException)
        {
            return;
        }

        // A no-op once the subscriber has answered, so the race between the two needs no guard.
        seat.ExpireRevoke(epoch);
    }

    /// <summary>
    /// Everything ready goes out in one body. That is the whole batching story: one record when idle
    /// costs one message, a backlog costs one message too, and there is no size to configure.
    /// </summary>
    private async ValueTask PushBatch(ITopicPartition[] partitions, bool[] owned, long[] cursors,
        CancellationToken cancellationToken)
    {
        var body = Connected().BeginOneWayBody(new PushRecordsMessage());
        try
        {
            var writer = new FetchedRecordWriter(body);
            for (short partition = 0; partition < partitions.Length; partition++)
            {
                if (!owned[partition])
                {
                    continue;
                }

                while (partitions[partition].TryReadRecord(0, partition, cursors[partition], out var record))
                {
                    await writer.WriteRecord(record, cancellationToken).ConfigureAwait(false);
                    cursors[partition]++;
                }
            }

            await writer.CompleteAsync(cancellationToken).ConfigureAwait(false);
        }
        finally
        {
            Connected().EndOneWayBody(body);
        }
    }

    private static bool HasRecords(ITopicPartition[] partitions, bool[] owned, long[] cursors)
    {
        for (var i = 0; i < partitions.Length; i++)
        {
            if (owned[i] && cursors[i] < partitions[i].Describe().EndOffset)
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
    private static async Task WaitForWork(ITopicPartition[] partitions, bool[] owned, long[] cursors,
        Task changed, CancellationToken cancellationToken)
    {
        using var linked = CancellationTokenSource.CreateLinkedTokenSource(cancellationToken);

        var waits = new List<Task>(partitions.Length);
        for (var i = 0; i < partitions.Length; i++)
        {
            if (owned[i])
            {
                waits.Add(partitions[i].WaitForAppend(linked.Token));
            }
        }

        // Re-checked only now: an append between the caller's look and here has already replaced the
        // signal these waits hold, so nothing but a second read could see it.
        if (!HasRecords(partitions, owned, cursors) && !changed.IsCompleted)
        {
            var appended = waits.Count > 0 ? Task.WhenAny(waits) : changed;
            await Task.WhenAny(appended, changed).ConfigureAwait(false);
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

    // Split out because a ref struct message cannot be built inside an async method.
    private static ValueTask SendRevoke(ConnectionBase connection, int epoch, short[] partitions,
        CancellationToken cancellationToken) =>
        connection.SendMessage(
            new RevokeMessage { Epoch = epoch, Partitions = new PartitionIds(partitions) }, cancellationToken);

    private ValueTask Lose(SubscriptionLostReason reason, CancellationToken cancellationToken) =>
        Connected().SendMessage(new SubscriptionLostMessage { Reason = reason }, cancellationToken);

    private ConnectionBase Connected() =>
        this.connection ?? throw new InvalidOperationException("The pump was run before it was attached.");
}
