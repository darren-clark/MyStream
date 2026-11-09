using System.Collections.Concurrent;
using ToyStream.Protocol.Messages;
using ToyStream.Shared;

namespace ToyStream.Server;

/// <summary>One revoke a pump has yet to write, and the epoch its answer must carry.</summary>
public readonly record struct PendingRevoke(int Epoch, short[] Partitions);

/// <summary>
/// Membership and partition assignment for the groups on this node. Node local by nature: a member
/// is a connection, so there is nothing here to persist and nothing to agree with anyone about.
/// </summary>
public sealed class ConsumerGroupCoordinator(IConsumerGroupService checkpoints)
{
    private readonly ConcurrentDictionary<GroupKey, ConsumerGroup> groups = new();

    private readonly record struct GroupKey(Utf8String Group, Utf8String Topic);

    /// <summary>
    /// Bounds the wait for a subscriber's final offsets. The drain narrows the redelivery window;
    /// it is not load bearing, so one unresponsive subscriber must not hold a partition forever.
    /// </summary>
    public TimeSpan DrainTimeout { get; init; } = TimeSpan.FromSeconds(30);

    /// <summary>
    /// How long a member that lost its connection keeps its partitions before the group gives up on
    /// it. A flapping subscriber otherwise costs two reassignments and a drain per flap, for a seat
    /// it is about to reclaim. Skipped entirely when a subscriber says it is leaving.
    /// </summary>
    public TimeSpan MemberLinger { get; init; } = TimeSpan.FromSeconds(5);

    public GroupMember Join(Utf8String group, Utf8String topic, Utf8String memberId, short partitionCount) =>
        this.groups
            .GetOrAdd(new GroupKey(group, topic), key => new ConsumerGroup(key.Group, key.Topic, partitionCount,
                checkpoints, MemberLinger))
            .Join(memberId);

    /// <summary>
    /// Who may push a partition right now, or null while it belongs to nobody - which is what a
    /// partition mid-handover is. Settled state, not the assignment that is merely desired.
    /// </summary>
    public Utf8String? OwnerOf(Utf8String group, Utf8String topic, short partition) =>
        this.groups.TryGetValue(new GroupKey(group, topic), out var found) ? found.OwnerOf(partition) : null;

    /// <summary>
    /// A subscriber with no group shares nothing, so it owns every partition for its whole life and
    /// takes the same loop as a member whose assignment simply never changes.
    /// </summary>
    public static GroupMember Alone(short partitionCount)
    {
        var all = new short[partitionCount];
        for (short i = 0; i < partitionCount; i++)
        {
            all[i] = i;
        }

        return new GroupMember(Utf8String.Empty, group: null, all);
    }
}

/// <summary>
/// A subscriber's seat in a group: what it may push, and what it has been asked to give back. The
/// pump reads this; the group writes it.
/// </summary>
/// <remarks>
/// Outlives the connection that created it. A member that drops and comes back on the same id gets
/// this same seat, which is what lets a reconnect cost nothing - and is why the id has to be stable.
/// </remarks>
public sealed class GroupMember
{
    private readonly ConsumerGroup? group;
    private readonly ConcurrentQueue<PendingRevoke> revokes = new();

    private short[] assigned;
    private TaskCompletionSource signal = new(TaskCreationOptions.RunContinuationsAsynchronously);
    private int epoch;
    private int departures;

    internal GroupMember(Utf8String id, ConsumerGroup? group, short[] assigned)
    {
        Id = id;
        this.group = group;
        this.assigned = assigned;
    }

    public Utf8String Id { get; }

    /// <summary>
    /// Which connection currently holds this seat. A reconnect can beat the server noticing the old
    /// connection died, so the departure of a superseded one must not evict the live pump.
    /// </summary>
    public int Attachment { get; private set; }

    public short[] Assigned => Volatile.Read(ref this.assigned);

    /// <summary>Holding its partitions, but with no connection to push them down.</summary>
    public bool IsDeparted { get; private set; }

    /// <summary>
    /// Snapshot this <em>before</em> reading the assignment: a change landing in between completes
    /// the task already in hand, where one taken afterwards would be missed.
    /// </summary>
    public Task Changed => Volatile.Read(ref this.signal).Task;

    public bool TryDequeueRevoke(out PendingRevoke revoke) => this.revokes.TryDequeue(out revoke);

    public bool Owns(short partition) => Array.IndexOf(Assigned, partition) >= 0;

    public void CompleteRevoke(int epoch, CommittedOffsets offsets) =>
        this.group?.CompleteRevoke(this, epoch, offsets);

    /// <summary>Hands the partition on from the last stored checkpoint, so nothing waits on a peer that went quiet.</summary>
    public void ExpireRevoke(int epoch) => this.group?.ExpireRevoke(this, epoch);

    /// <summary>
    /// Lost its connection. Keeps its partitions for the linger, because a subscriber that comes
    /// straight back should not have cost the group two handovers in the meantime.
    /// </summary>
    public void Depart(int attachment = 0) => this.group?.Depart(this, attachment);

    /// <summary>Said it was going, so there is nothing to wait for.</summary>
    public void Leave(int attachment = 0) => this.group?.Leave(this, attachment);

    internal int NextEpoch() => Interlocked.Increment(ref this.epoch);

    internal int Attach() => ++Attachment;

    internal bool IsCurrent(int attachment) => attachment == 0 || attachment == Attachment;

    internal void Assign(short[] partitions) => Volatile.Write(ref this.assigned, partitions);

    internal void Enqueue(PendingRevoke revoke) => this.revokes.Enqueue(revoke);

    /// <summary>Returns the generation, so an expiry can tell its own departure from a later one.</summary>
    internal int MarkDeparted()
    {
        IsDeparted = true;
        return ++this.departures;
    }

    internal bool IsDepartureCurrent(int generation) => IsDeparted && this.departures == generation;

    internal void MarkReturned() => IsDeparted = false;

    /// <summary>Swapped before the old one completes, so a waiter always holds a live signal.</summary>
    internal void Signal() =>
        Interlocked.Exchange(ref this.signal, new TaskCompletionSource(TaskCreationOptions.RunContinuationsAsynchronously))
            .TrySetResult();
}

/// <summary>
/// One group's view of one topic. Assignment is a pure function of the partition and the sorted
/// member list, recomputed from scratch whenever either changes; it says what <em>should</em> be,
/// never what may be pushed. Only <see cref="owners"/> authorises a push, and it advances a
/// partition at a time through a revoke and its answer.
/// </summary>
internal sealed class ConsumerGroup(Utf8String group, Utf8String topic, short partitionCount,
    IConsumerGroupService checkpoints, TimeSpan linger)
{
    private readonly Lock gate = new();
    private readonly List<GroupMember> members = [];

    /// <summary>Who may push each partition right now, which is never ahead of a finished handover.</summary>
    private readonly GroupMember?[] owners = new GroupMember?[partitionCount];

    private readonly GroupMember?[] revokingFrom = new GroupMember?[partitionCount];
    private readonly int[] revokeEpoch = new int[partitionCount];

    public GroupMember Join(Utf8String memberId)
    {
        lock (this.gate)
        {
            // The same id gets the same seat, so a reconnect inside the linger moves nothing.
            foreach (var existing in this.members)
            {
                if (existing.Id.Equals(memberId))
                {
                    existing.MarkReturned();
                    existing.Attach();
                    Rebalance();
                    return existing;
                }
            }

            var joined = new GroupMember(memberId, this, []);
            joined.Attach();
            this.members.Add(joined);

            // Sorted by id so every node, and every recompute, lays the group out the same way.
            this.members.Sort(static (left, right) => left.Id.CompareTo(right.Id));

            Rebalance();
            return joined;
        }
    }

    /// <summary>
    /// The connection went without saying so. Its partitions stay put for the linger rather than
    /// being handed round and handed straight back, which is what a flapping subscriber would cost.
    /// </summary>
    public void Depart(GroupMember member, int attachment)
    {
        int generation;
        lock (this.gate)
        {
            // Superseded by a reconnect that got in first, so this departure is already history.
            if (!this.members.Contains(member) || !member.IsCurrent(attachment))
            {
                return;
            }

            generation = member.MarkDeparted();
            ReleaseUnanswerable(member);
            Rebalance();
        }

        _ = ExpireDeparture(member, generation);
    }

    public void Leave(GroupMember member, int attachment)
    {
        lock (this.gate)
        {
            if (!member.IsCurrent(attachment) || !this.members.Remove(member))
            {
                return;
            }

            ReleaseUnanswerable(member);
            Disown(member);
            Rebalance();
        }
    }

    /// <summary>
    /// A revoke aimed at a member that has gone can never be answered, so it is released now rather
    /// than left to a drain timeout that nothing is going to fire.
    /// </summary>
    private void ReleaseUnanswerable(GroupMember member)
    {
        for (short partition = 0; partition < partitionCount; partition++)
        {
            if (ReferenceEquals(this.revokingFrom[partition], member))
            {
                Release(partition);
            }
        }
    }

    private void Disown(GroupMember member)
    {
        for (short partition = 0; partition < partitionCount; partition++)
        {
            if (ReferenceEquals(this.owners[partition], member))
            {
                this.owners[partition] = null;
            }
        }
    }

    private async Task ExpireDeparture(GroupMember member, int generation)
    {
        await Task.Delay(linger).ConfigureAwait(false);

        lock (this.gate)
        {
            // A no-op once it is back, or once a later departure has replaced this one.
            if (!member.IsDepartureCurrent(generation))
            {
                return;
            }

            this.members.Remove(member);
            Disown(member);
            Rebalance();
        }
    }

    public void CompleteRevoke(GroupMember member, int epoch, CommittedOffsets offsets)
    {
        lock (this.gate)
        {
            for (short partition = 0; partition < partitionCount; partition++)
            {
                if (!IsRevoking(partition, member, epoch))
                {
                    continue;
                }

                foreach (var offset in offsets)
                {
                    if (offset.PartitionId == partition)
                    {
                        checkpoints.Commit(group, topic, partition, offset.Offset);
                    }
                }

                Release(partition);
            }

            Rebalance();
        }
    }

    public void ExpireRevoke(GroupMember member, int epoch)
    {
        lock (this.gate)
        {
            for (short partition = 0; partition < partitionCount; partition++)
            {
                if (IsRevoking(partition, member, epoch))
                {
                    Release(partition);
                }
            }

            Rebalance();
        }
    }

    private bool IsRevoking(short partition, GroupMember member, int epoch) =>
        ReferenceEquals(this.revokingFrom[partition], member) && this.revokeEpoch[partition] == epoch;

    /// <summary>Null while a handover is in flight: the partition belongs to neither side then.</summary>
    public Utf8String? OwnerOf(short partition)
    {
        lock (this.gate)
        {
            return this.revokingFrom[partition] is null ? this.owners[partition]?.Id : null;
        }
    }

    private void Release(short partition)
    {
        this.revokingFrom[partition] = null;
        this.owners[partition] = null;
    }

    /// <summary>
    /// Recomputed whole rather than patched, so it converges on the current membership instead of
    /// replaying the changes that got there. Overlapping rebalances therefore coalesce.
    /// </summary>
    private void Rebalance()
    {
        Dictionary<GroupMember, List<short>>? revoking = null;

        for (short partition = 0; partition < partitionCount; partition++)
        {
            // A handover already under way decides its own partition; it is revisited on completion.
            if (this.revokingFrom[partition] is not null)
            {
                continue;
            }

            var desired = this.members.Count == 0 ? null : this.members[partition % this.members.Count];
            var owner = this.owners[partition];
            if (ReferenceEquals(owner, desired))
            {
                continue;
            }

            if (owner is null)
            {
                this.owners[partition] = desired;
                continue;
            }

            // A seat with no connection cannot answer a revoke, so it hands over on the spot. The
            // new owner resumes from the stored checkpoint, exactly as it would on a drain timeout.
            if (owner.IsDeparted)
            {
                this.owners[partition] = desired;
                continue;
            }

            this.revokingFrom[partition] = owner;
            revoking ??= [];
            if (!revoking.TryGetValue(owner, out var batch))
            {
                revoking[owner] = batch = [];
            }

            batch.Add(partition);
        }

        if (revoking is not null)
        {
            // One revoke per member per rebalance: a batch costs one drain window where one each
            // would cost as many, and the whole batch shares an epoch.
            foreach (var (member, batch) in revoking)
            {
                var epoch = member.NextEpoch();
                foreach (var partition in batch)
                {
                    this.revokeEpoch[partition] = epoch;
                }

                member.Enqueue(new PendingRevoke(epoch, batch.ToArray()));
                member.Signal();
            }
        }

        PublishAssignments();
    }

    /// <summary>Only what a member owns outright: a partition mid-revoke belongs to neither side.</summary>
    private void PublishAssignments()
    {
        foreach (var member in this.members)
        {
            // A departed seat still holds its partitions, but there is no pump to tell about them.
            if (member.IsDeparted)
            {
                continue;
            }

            var owned = new List<short>();
            for (short partition = 0; partition < partitionCount; partition++)
            {
                if (ReferenceEquals(this.owners[partition], member) && this.revokingFrom[partition] is null)
                {
                    owned.Add(partition);
                }
            }

            var next = owned.ToArray();
            if (next.AsSpan().SequenceEqual(member.Assigned))
            {
                continue;
            }

            member.Assign(next);
            member.Signal();
        }
    }
}
