namespace ToyStream.Server.Test;

using ToyStream.Protocol.Messages;
using ToyStream.Server.InMemory;
using ToyStream.Shared;

/// <summary>
/// The coordinator on its own, with no connection in the way: assignment is a pure function of the
/// partition and the sorted members, and only a finished handover moves ownership.
/// </summary>
public class ConsumerGroupTests
{
    private static readonly Utf8String Group = new("readers");
    private static readonly Utf8String Topic = new("events");

    [Test]
    public void TheFirstMemberTakesEveryPartition()
    {
        var coordinator = new ConsumerGroupCoordinator(new InMemoryConsumerGroupService());

        var first = coordinator.Join(Group, Topic, new Utf8String("a"), 2);

        Assert.That(first.Assigned, Is.EqualTo(new short[] { 0, 1 }));
        Assert.That(Owner(coordinator, 0), Is.EqualTo("a"));
        Assert.That(Owner(coordinator, 1), Is.EqualTo("a"));
    }

    [Test]
    public void AJoinRevokesRatherThanReassigning()
    {
        var coordinator = new ConsumerGroupCoordinator(new InMemoryConsumerGroupService());
        var first = coordinator.Join(Group, Topic, new Utf8String("a"), 2);

        var second = coordinator.Join(Group, Topic, new Utf8String("b"), 2);

        Assert.Multiple(() =>
        {
            Assert.That(first.TryDequeueRevoke(out var revoke), Is.True, "no revoke was raised");
            Assert.That(revoke.Partitions, Is.EqualTo(new short[] { 1 }));
            Assert.That(second.Assigned, Is.Empty, "the partition moved before it was given up");
            Assert.That(Owner(coordinator, 1), Is.Null, "a partition mid-handover belongs to nobody");
        });
    }

    [Test]
    public void OwnershipMovesOnlyWhenTheRevokeIsAnswered()
    {
        var coordinator = new ConsumerGroupCoordinator(new InMemoryConsumerGroupService());
        var first = coordinator.Join(Group, Topic, new Utf8String("a"), 2);
        var second = coordinator.Join(Group, Topic, new Utf8String("b"), 2);
        Assert.That(first.TryDequeueRevoke(out var revoke), Is.True);

        Complete(first, revoke.Epoch, 1, offset: 7);

        Assert.Multiple(() =>
        {
            Assert.That(Owner(coordinator, 1), Is.EqualTo("b"));
            Assert.That(second.Assigned, Is.EqualTo(new short[] { 1 }));
            Assert.That(first.Assigned, Is.EqualTo(new short[] { 0 }));
        });
    }

    [Test]
    public void TheAnsweredOffsetBecomesTheCheckpoint()
    {
        var checkpoints = new InMemoryConsumerGroupService();
        var coordinator = new ConsumerGroupCoordinator(checkpoints);
        var first = coordinator.Join(Group, Topic, new Utf8String("a"), 2);
        coordinator.Join(Group, Topic, new Utf8String("b"), 2);
        Assert.That(first.TryDequeueRevoke(out var revoke), Is.True);

        Complete(first, revoke.Epoch, 1, offset: 7);

        Assert.That(checkpoints.TryGetCommittedOffset(Group, Topic, 1, out var committed), Is.True);
        Assert.That(committed, Is.EqualTo(7));
    }

    /// <summary>An answer to a revoke that has already expired must not release the next one.</summary>
    [Test]
    public void AStaleEpochIsIgnored()
    {
        var coordinator = new ConsumerGroupCoordinator(new InMemoryConsumerGroupService());
        var first = coordinator.Join(Group, Topic, new Utf8String("a"), 2);
        var second = coordinator.Join(Group, Topic, new Utf8String("b"), 2);
        Assert.That(first.TryDequeueRevoke(out var revoke), Is.True);

        Complete(first, revoke.Epoch - 1, 1, offset: 7);

        Assert.That(Owner(coordinator, 1), Is.Null, "a stale answer finished the handover");
        Assert.That(second.Assigned, Is.Empty);
    }

    [Test]
    public void ExpiringADrainHandsThePartitionOnAnyway()
    {
        var coordinator = new ConsumerGroupCoordinator(new InMemoryConsumerGroupService());
        var first = coordinator.Join(Group, Topic, new Utf8String("a"), 2);
        coordinator.Join(Group, Topic, new Utf8String("b"), 2);
        Assert.That(first.TryDequeueRevoke(out var revoke), Is.True);

        first.ExpireRevoke(revoke.Epoch);

        Assert.That(Owner(coordinator, 1), Is.EqualTo("b"));
    }

    [Test]
    public void LeavingFreesPartitionsWithoutADrain()
    {
        var coordinator = new ConsumerGroupCoordinator(new InMemoryConsumerGroupService());
        var first = coordinator.Join(Group, Topic, new Utf8String("a"), 2);
        var second = coordinator.Join(Group, Topic, new Utf8String("b"), 2);
        Assert.That(first.TryDequeueRevoke(out var revoke), Is.True);
        Complete(first, revoke.Epoch, 1, offset: 7);

        first.Leave();

        Assert.That(second.Assigned, Is.EqualTo(new short[] { 0, 1 }));
    }

    /// <summary>Sorted by id, so the layout does not depend on who turned up first.</summary>
    [Test]
    public void AssignmentDoesNotDependOnJoinOrder()
    {
        var coordinator = new ConsumerGroupCoordinator(new InMemoryConsumerGroupService());
        var second = coordinator.Join(Group, Topic, new Utf8String("b"), 2);
        var first = coordinator.Join(Group, Topic, new Utf8String("a"), 2);

        Assert.That(second.TryDequeueRevoke(out var revoke), Is.True);
        Complete(second, revoke.Epoch, 0, offset: 0);

        Assert.Multiple(() =>
        {
            Assert.That(Owner(coordinator, 0), Is.EqualTo("a"));
            Assert.That(Owner(coordinator, 1), Is.EqualTo("b"));
            Assert.That(first.Assigned, Is.EqualTo(new short[] { 0 }));
        });
    }

    /// <summary>
    /// The member the partition was going to has gone, but the revoke is already in flight. It has
    /// to be seen through rather than torn up, or the answer would land on a partition that had
    /// moved on behind it.
    /// </summary>
    [Test]
    public void AMemberLeavingMidRevokeReturnsThePartitionOnTheAnswer()
    {
        var coordinator = new ConsumerGroupCoordinator(new InMemoryConsumerGroupService());
        var first = coordinator.Join(Group, Topic, new Utf8String("a"), 2);
        var second = coordinator.Join(Group, Topic, new Utf8String("b"), 2);
        Assert.That(first.TryDequeueRevoke(out var revoke), Is.True);

        second.Leave();
        Assert.That(Owner(coordinator, 1), Is.Null, "the partition was handed back before it was given up");

        Complete(first, revoke.Epoch, 1, offset: 4);

        Assert.That(Owner(coordinator, 1), Is.EqualTo("a"));
        Assert.That(first.Assigned, Is.EqualTo(new short[] { 0, 1 }));
    }

    /// <summary>The same, for a subscriber that never answers: the timeout has to cover it too.</summary>
    [Test]
    public void AMemberLeavingMidRevokeReturnsThePartitionOnTimeout()
    {
        var coordinator = new ConsumerGroupCoordinator(new InMemoryConsumerGroupService());
        var first = coordinator.Join(Group, Topic, new Utf8String("a"), 2);
        var second = coordinator.Join(Group, Topic, new Utf8String("b"), 2);
        Assert.That(first.TryDequeueRevoke(out var revoke), Is.True);
        second.Leave();

        first.ExpireRevoke(revoke.Epoch);

        Assert.That(Owner(coordinator, 1), Is.EqualTo("a"));
    }

    /// <summary>
    /// A member that flaps straight back must not stack a second revoke on a partition already
    /// being given up, or the first answer would release a handover the second is still waiting on.
    /// </summary>
    [Test]
    public void RejoiningDuringARevokeDoesNotRaiseASecondRevoke()
    {
        var coordinator = new ConsumerGroupCoordinator(new InMemoryConsumerGroupService());
        var first = coordinator.Join(Group, Topic, new Utf8String("a"), 2);
        var second = coordinator.Join(Group, Topic, new Utf8String("b"), 2);
        Assert.That(first.TryDequeueRevoke(out _), Is.True);

        second.Leave();
        coordinator.Join(Group, Topic, new Utf8String("b"), 2);

        Assert.That(first.TryDequeueRevoke(out _), Is.False, "the flap raised a second revoke");
    }

    /// <summary>
    /// The case the epoch exists for, reached the way it actually happens: a partition is revoked,
    /// times out, comes back, and is revoked again, leaving the first answer still in flight.
    /// </summary>
    [Test]
    public void AnAnswerFromAPreviousRevokeCycleIsIgnored()
    {
        var checkpoints = new InMemoryConsumerGroupService();
        var coordinator = new ConsumerGroupCoordinator(checkpoints);
        var first = coordinator.Join(Group, Topic, new Utf8String("a"), 2);
        var second = coordinator.Join(Group, Topic, new Utf8String("b"), 2);

        Assert.That(first.TryDequeueRevoke(out var stale), Is.True);
        first.ExpireRevoke(stale.Epoch);
        second.Leave();

        // Back with "a", and taken off it again, so the first answer is now a cycle behind.
        coordinator.Join(Group, Topic, new Utf8String("b"), 2);
        Assert.That(first.TryDequeueRevoke(out var current), Is.True);
        Assert.That(current.Epoch, Is.Not.EqualTo(stale.Epoch));

        Complete(first, stale.Epoch, 1, offset: 99);

        Assert.Multiple(() =>
        {
            Assert.That(Owner(coordinator, 1), Is.Null, "a stale answer finished the current handover");
            Assert.That(checkpoints.TryGetCommittedOffset(Group, Topic, 1, out _), Is.False,
                "a stale answer moved the checkpoint");
        });

        Complete(first, current.Epoch, 1, offset: 5);

        Assert.That(Owner(coordinator, 1), Is.EqualTo("b"));
        Assert.That(checkpoints.TryGetCommittedOffset(Group, Topic, 1, out var committed), Is.True);
        Assert.That(committed, Is.EqualTo(5));
    }

    /// <summary>Churn has to settle, not accumulate: the survivor ends up owning everything.</summary>
    [Test]
    public void RepeatedFlappingSettlesBackOnTheRemainingMember()
    {
        var coordinator = new ConsumerGroupCoordinator(new InMemoryConsumerGroupService());
        var first = coordinator.Join(Group, Topic, new Utf8String("a"), 2);

        for (var cycle = 0; cycle < 20; cycle++)
        {
            var flapping = coordinator.Join(Group, Topic, new Utf8String("b"), 2);
            Assert.That(first.TryDequeueRevoke(out var revoke), Is.True, $"no revoke on cycle {cycle}");
            Complete(first, revoke.Epoch, 1, offset: cycle);

            flapping.Leave();
        }

        Assert.Multiple(() =>
        {
            Assert.That(Owner(coordinator, 0), Is.EqualTo("a"));
            Assert.That(Owner(coordinator, 1), Is.EqualTo("a"));
            Assert.That(first.Assigned, Is.EqualTo(new short[] { 0, 1 }));
            Assert.That(first.TryDequeueRevoke(out _), Is.False, "a revoke outlived the member that caused it");
        });
    }

    /// <summary>
    /// A lost connection is not the same as a departure. The seat keeps its partitions, because a
    /// subscriber that is coming straight back should not cost two handovers on the way.
    /// </summary>
    [Test]
    public void ADepartedMemberKeepsItsPartitionsForTheLinger()
    {
        var coordinator = Coordinating(linger: TimeSpan.FromMinutes(1));
        var first = coordinator.Join(Group, Topic, new Utf8String("a"), 2);
        var second = Settled(coordinator, first);

        second.Depart();

        Assert.That(Owner(coordinator, 1), Is.EqualTo("b"), "the seat was given up before the linger");
        Assert.That(first.Assigned, Is.EqualTo(new short[] { 0 }), "the partition was handed on early");
    }

    /// <summary>The whole point: coming back on the same id moves nothing at all.</summary>
    [Test]
    public void ReturningInsideTheLingerCostsNoHandover()
    {
        var coordinator = Coordinating(linger: TimeSpan.FromMinutes(1));
        var first = coordinator.Join(Group, Topic, new Utf8String("a"), 2);
        var second = Settled(coordinator, first);
        second.Depart();

        var returned = coordinator.Join(Group, Topic, new Utf8String("b"), 2);

        Assert.Multiple(() =>
        {
            Assert.That(returned, Is.SameAs(second), "the returning member was given a new seat");
            Assert.That(returned.Assigned, Is.EqualTo(new short[] { 1 }));
            Assert.That(Owner(coordinator, 1), Is.EqualTo("b"));
            Assert.That(first.TryDequeueRevoke(out _), Is.False, "the reconnect cost a revoke");
        });
    }

    [Test]
    public async Task TheLingerExpiryHandsThePartitionsOn()
    {
        var coordinator = Coordinating(linger: TimeSpan.FromMilliseconds(100));
        var first = coordinator.Join(Group, Topic, new Utf8String("a"), 2);
        var second = Settled(coordinator, first);

        second.Depart();
        await Task.Delay(TimeSpan.FromSeconds(1));

        Assert.That(Owner(coordinator, 1), Is.EqualTo("a"));
        Assert.That(first.Assigned, Is.EqualTo(new short[] { 0, 1 }));
    }

    /// <summary>Saying so is what makes a shutdown immediate rather than a five second stall.</summary>
    [Test]
    public void AnExplicitLeaveSkipsTheLinger()
    {
        var coordinator = Coordinating(linger: TimeSpan.FromMinutes(1));
        var first = coordinator.Join(Group, Topic, new Utf8String("a"), 2);
        var second = Settled(coordinator, first);

        second.Leave();

        Assert.That(Owner(coordinator, 1), Is.EqualTo("a"));
    }

    /// <summary>
    /// A departed seat cannot answer a revoke, so a partition wanted off it moves on the spot rather
    /// than waiting out a drain that nothing would ever fire.
    /// </summary>
    [Test]
    public void APartitionWantedOffADepartedMemberMovesWithoutARevoke()
    {
        var coordinator = Coordinating(linger: TimeSpan.FromMinutes(1));
        var first = coordinator.Join(Group, Topic, new Utf8String("a"), 2);
        var second = Settled(coordinator, first);

        first.Depart();

        // "a" is gone, so with two members left by id the sort puts partition 0 on "b" regardless.
        coordinator.Join(Group, Topic, new Utf8String("c"), 2);

        Assert.That(Owner(coordinator, 0), Is.Not.Null, "partition 0 was left mid-handover with nobody to answer");
        Assert.That(second.TryDequeueRevoke(out _), Is.False, "a revoke was raised against a departed seat");
    }

    private static ConsumerGroupCoordinator Coordinating(TimeSpan linger) =>
        new(new InMemoryConsumerGroupService()) { MemberLinger = linger };

    /// <summary>Joins "b" and completes the handover it triggers, leaving one partition each.</summary>
    private static GroupMember Settled(ConsumerGroupCoordinator coordinator, GroupMember first)
    {
        var second = coordinator.Join(Group, Topic, new Utf8String("b"), 2);
        Assert.That(first.TryDequeueRevoke(out var revoke), Is.True);
        Complete(first, revoke.Epoch, 1, offset: 0);
        return second;
    }

    private static string? Owner(ConsumerGroupCoordinator coordinator, short partition)    {
        var owner = coordinator.OwnerOf(Group, Topic, partition);
        return owner is null ? null : (string)owner;
    }

    // Split out because a ref struct cannot be built inside a method that also asserts asynchronously.
    private static void Complete(GroupMember member, int epoch, short partition, long offset) =>
        member.CompleteRevoke(epoch, new CommittedOffsets([new CommittedOffset(partition, offset)]));
}
