namespace ToyStream.Client;

using System.IO.Pipelines;
using ToyStream.Protocol;
using ToyStream.Protocol.Messages;
using ToyStream.Shared;

public sealed record SubscriberOptions
{
    public string Topic { get; init; } = string.Empty;

    /// <summary>
    /// Empty for a subscriber that keeps no checkpoint. A group is what a committed offset is keyed
    /// by, so without one there is nowhere to resume from.
    /// </summary>
    public string Group { get; init; } = string.Empty;

    public SubscribeOffset StartAt { get; init; } = SubscribeOffset.Earliest;

    /// <summary>
    /// Sorts this subscriber within its group, so it wants to be stable across a restart - a
    /// returning subscriber then reclaims its own partitions instead of reshuffling everyone.
    /// Generated per instance when left empty, which is only right for a subscriber that never
    /// expects to come back as itself.
    /// </summary>
    public string MemberId { get; init; } = string.Empty;

    /// <summary>
    /// Treats asking for the next record as finishing the previous one, which is what the loop body
    /// of an <c>await foreach</c> means anyway.
    /// </summary>
    public bool AutoCommit { get; init; } = true;
}

public interface IToyStreamSubscriber : IAsyncDisposable
{
    /// <summary>Everything about the record in hand except its body.</summary>
    FetchedRecordHeader Current { get; }

    ReadOnlyMemory<byte> Key { get; }

    ValueTask ConnectAsync(CancellationToken cancellationToken = default);

    ValueTask<bool> MoveNextAsync(CancellationToken cancellationToken = default);

    ValueTask CopyBodyTo(PipeWriter destination, CancellationToken cancellationToken = default);

    ValueTask CommitAsync(CancellationToken cancellationToken = default);
}

/// <summary>
/// Reads records the server pushes, rather than asking for them. A cursor like
/// <see cref="ToyStreamConsumer"/>, and for the same reason: the record in hand is streamed straight
/// off the connection, so it is only valid until the next move.
/// </summary>
/// <remarks>
/// Not thread safe, and it holds its own connection: once subscribed, that connection only ever
/// carries records the other way.
/// </remarks>
public sealed class ToyStreamSubscriber : IToyStreamSubscriber, IRevocationSink, IAsyncDisposable
{
    private readonly SubscriptionStream stream;
    private readonly ClientConnection<SubscriberClientHandler> connection;
    private readonly SubscriberOptions options;
    private readonly Utf8String topic;
    private readonly Utf8String group;
    private readonly Utf8String memberId;

    /// <summary>
    /// Serialises this subscriber's two writers. Commits go out on the caller's thread and a revoke
    /// is answered from the read loop, and <see cref="ConnectionBase"/> throws on an overlap rather
    /// than waiting for one.
    /// </summary>
    private readonly SemaphoreSlim writing = new(1, 1);

    /// <summary>
    /// The next offset wanted per partition: everything this subscriber has committed, kept whole
    /// rather than as a delta so a revoke can be answered from the same place a flush reads.
    /// </summary>
    private readonly Dictionary<short, long> positions = new();

    private readonly Lock gate = new();

    private short completedPartition = -1;
    private long completedOffset;
    private bool owed;

    private Task? watching;
    private bool subscribed;

    public ToyStreamSubscriber(IClientTransport transport, SubscriberOptions options)
    {
        ArgumentException.ThrowIfNullOrEmpty(options.Topic);

        if (options.StartAt == SubscribeOffset.Committed && string.IsNullOrEmpty(options.Group))
        {
            throw new ArgumentException("A committed start offset needs a group to be keyed by.", nameof(options));
        }

        this.topic = new Utf8String(options.Topic);
        this.group = new Utf8String(options.Group);
        this.memberId = new Utf8String(string.IsNullOrEmpty(options.MemberId)
            ? Guid.NewGuid().ToString("n")
            : options.MemberId);
        this.options = options;
        this.stream = new SubscriptionStream(options.AutoCommit ? FlushCommits : null);
        this.connection = new ClientConnection<SubscriberClientHandler>(
            transport, ConnectionRole.Subscriber, new SubscriberClientHandler(this.stream, this));
    }

    /// <summary>
    /// Connects and subscribes. Expected to be awaited up front, though a read still does it on
    /// demand; an unknown topic surfaces at the first <see cref="MoveNextAsync"/> rather than here,
    /// because subscribing is not answered.
    /// </summary>
    public async ValueTask ConnectAsync(CancellationToken cancellationToken = default) =>
        await EnsureSubscribed(cancellationToken).ConfigureAwait(false);

    /// <summary>
    /// Advances to the next record, waiting for one to arrive. False only once the subscription has
    /// ended, which for a server-side refusal is a <see cref="SubscriptionLostException"/>.
    /// </summary>
    public async ValueTask<bool> MoveNextAsync(CancellationToken cancellationToken = default)
    {
        await EnsureSubscribed(cancellationToken).ConfigureAwait(false);

        // Before the move, so a batch draining inside it already accounts for the record just left.
        if (this.options.AutoCommit)
        {
            MarkCompleted();
        }

        if (!await this.stream.MoveNextAsync(cancellationToken).ConfigureAwait(false))
        {
            return false;
        }

        this.completedPartition = Current.PartitionId;
        this.completedOffset = Current.Offset;
        return true;
    }

    /// <summary>
    /// Commits through the record in hand. For a subscriber that defers its work past the next move,
    /// which is the one case auto committing would checkpoint records it has not finished.
    /// </summary>
    public ValueTask CommitAsync(CancellationToken cancellationToken = default)
    {
        MarkCompleted();
        return FlushCommits(cancellationToken);
    }

    public FetchedRecordHeader Current => this.stream.Records.Current;

    public ReadOnlyMemory<byte> Key => this.stream.Records.Key;

    /// <summary>
    /// Streams the current record's body out. Unbounded, and valid only until the next move, so a
    /// caller that wants to keep it copies it here.
    /// </summary>
    public ValueTask CopyBodyTo(PipeWriter destination, CancellationToken cancellationToken = default) =>
        this.stream.Records.CopyBodyTo(destination, cancellationToken);

    public async ValueTask DisposeAsync()
    {
        // A clean exit is a handover rather than a timeout, so whatever is owed goes out first, and
        // saying so is what stops the group holding this seat open in case it comes back.
        try
        {
            MarkCompleted();
            await FlushCommits(CancellationToken.None).ConfigureAwait(false);
            await SayGoodbye(CancellationToken.None).ConfigureAwait(false);
        }
        catch (Exception) when (this.subscribed)
        {
            // The connection is already going; redelivery is the cost and the caller is leaving.
        }

        // Before the connection goes: the read loop may be parked inside a handler holding a batch,
        // and would otherwise never return to notice the cancellation.
        this.stream.Release();
        this.stream.Close();

        await this.connection.DisposeAsync().ConfigureAwait(false);

        if (this.watching is { } watch)
        {
            await watch.ConfigureAwait(false);
        }

        this.writing.Dispose();
    }

    private void MarkCompleted()
    {
        lock (this.gate)
        {
            if (this.completedPartition < 0)
            {
                return;
            }

            // The next offset wanted, not the last one seen: that is what a resume reads from.
            this.positions[this.completedPartition] = this.completedOffset + 1;
            this.completedPartition = -1;
            this.owed = true;
        }
    }

    private async ValueTask FlushCommits(CancellationToken cancellationToken)
    {
        if (!this.subscribed || this.group.Memory.IsEmpty)
        {
            return;
        }

        CommittedOffset[] snapshot;
        lock (this.gate)
        {
            if (!this.owed)
            {
                return;
            }

            this.owed = false;
            snapshot = Snapshot(this.positions);
        }

        await Write(static (connection, state, token) => SendCommit(connection, state.Offsets, token),
            (Offsets: (ReadOnlyMemory<CommittedOffset>)snapshot, Epoch: 0), cancellationToken).ConfigureAwait(false);
    }

    /// <summary>
    /// Answers with what was committed rather than where the cursor got to, so a subscriber that
    /// defers its work cannot hand on a position it never actually reached.
    /// </summary>
    async ValueTask IRevocationSink.Revoked(int epoch, short[] partitions, CancellationToken cancellationToken)
    {
        CommittedOffset[] answer;
        lock (this.gate)
        {
            // Dropped as they are answered: they belong to someone else from here on, and a later
            // flush naming them would be rejected anyway.
            var found = new List<CommittedOffset>(partitions.Length);
            foreach (var partition in partitions)
            {
                if (this.positions.Remove(partition, out var offset))
                {
                    found.Add(new CommittedOffset(partition, offset));
                }
            }

            if (this.completedPartition >= 0 && Array.IndexOf(partitions, this.completedPartition) >= 0)
            {
                this.completedPartition = -1;
            }

            answer = found.ToArray();
        }

        await Write(static (connection, state, token) => SendRevokeComplete(connection, state.Epoch, state.Offsets, token),
            (Offsets: (ReadOnlyMemory<CommittedOffset>)answer, Epoch: epoch), cancellationToken).ConfigureAwait(false);
    }

    private async ValueTask SayGoodbye(CancellationToken cancellationToken)
    {
        if (!this.subscribed || this.group.Memory.IsEmpty)
        {
            return;
        }

        await this.writing.WaitAsync(cancellationToken).ConfigureAwait(false);
        try
        {
            var connected = await this.connection.EnsureConnected(cancellationToken).ConfigureAwait(false);
            await connected.SendMessage(new LeaveGroupMessage(), cancellationToken).ConfigureAwait(false);
        }
        finally
        {
            this.writing.Release();
        }
    }

    private static CommittedOffset[] Snapshot(Dictionary<short, long> positions)
    {
        var snapshot = new CommittedOffset[positions.Count];
        var count = 0;
        foreach (var (partition, offset) in positions)
        {
            snapshot[count++] = new CommittedOffset(partition, offset);
        }

        return snapshot;
    }

    private async ValueTask Write(
        Func<ConnectionBase, (ReadOnlyMemory<CommittedOffset> Offsets, int Epoch), CancellationToken, ValueTask> send,
        (ReadOnlyMemory<CommittedOffset> Offsets, int Epoch) state, CancellationToken cancellationToken)
    {
        await this.writing.WaitAsync(cancellationToken).ConfigureAwait(false);
        try
        {
            var connected = await this.connection.EnsureConnected(cancellationToken).ConfigureAwait(false);
            await send(connected, state, cancellationToken).ConfigureAwait(false);
        }
        finally
        {
            this.writing.Release();
        }
    }

    private async ValueTask EnsureSubscribed(CancellationToken cancellationToken)
    {
        var connected = await this.connection.EnsureConnected(cancellationToken).ConfigureAwait(false);
        if (this.subscribed)
        {
            return;
        }

        // Latched only once the server has been told, so a failed send is retried rather than
        // leaving every later read waiting on a subscription that was never made.
        await SendSubscribe(connected, this.topic, this.group, this.memberId, this.options.StartAt,
            cancellationToken).ConfigureAwait(false);
        this.subscribed = true;

        // A one-way connection has nothing else that would ever fail, so the read loop ending is the
        // only sign the server has gone.
        this.watching = WatchConnection(this.connection, this.stream);
    }

    private static async Task WatchConnection(ClientConnection<SubscriberClientHandler> connection,
        SubscriptionStream stream)
    {
        try
        {
            await connection.ReadLoop.ConfigureAwait(false);
            stream.Close();
        }
        catch (Exception exception)
        {
            stream.Close(exception);
        }
    }

    // Split out because a ref struct message cannot be built inside an async method.
    private static ValueTask SendSubscribe(ConnectionBase connection, Utf8String topic, Utf8String group,
        Utf8String memberId, SubscribeOffset startAt, CancellationToken cancellationToken) =>
        connection.SendMessage(
            new SubscribeMessage { Topic = topic, Group = group, MemberId = memberId, StartOffset = startAt },
            cancellationToken);

    private static ValueTask SendCommit(ConnectionBase connection, ReadOnlyMemory<CommittedOffset> offsets,
        CancellationToken cancellationToken) =>
        connection.SendMessage(
            new CommitOffsetsMessage { Offsets = new CommittedOffsets(offsets.Span) }, cancellationToken);

    private static ValueTask SendRevokeComplete(ConnectionBase connection, int epoch,
        ReadOnlyMemory<CommittedOffset> offsets, CancellationToken cancellationToken) =>
        connection.SendMessage(
            new RevokeCompleteMessage { Epoch = epoch, Offsets = new CommittedOffsets(offsets.Span) },
            cancellationToken);
}
