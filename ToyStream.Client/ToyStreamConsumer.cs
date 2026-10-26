namespace ToyStream.Client;

using System.Diagnostics;
using System.Diagnostics.CodeAnalysis;
using System.IO.Pipelines;
using ToyStream.Protocol;
using ToyStream.Protocol.Messages;
using ToyStream.Protocol.Responses;
using ToyStream.Shared;

public enum StartPosition
{
    Earliest,
    Latest
}

public sealed record ConsumerOptions
{
    public StartPosition StartAt { get; init; } = StartPosition.Earliest;

    /// <summary>How long the server may hold a fetch open before answering with nothing.</summary>
    public int MaxWaitMilliseconds { get; init; } = 5_000;

    public int MaxBytes { get; init; } = 1024 * 1024;
}

/// <summary>
/// Reads records across a fixed set of topics. A cursor rather than a callback: the body of the
/// record in hand is streamed straight off the connection, so it is only valid until the next move.
/// </summary>
/// <remarks>
/// Not thread safe, and a fetch occupies the connection for as long as it is parked, which is why
/// this holds its own rather than sharing the admin client's.
/// </remarks>
public sealed class ToyStreamConsumer : IAsyncDisposable
{
    private readonly ClientConnection<ClientHandler> connection;
    private readonly ConsumerOptions options;
    private readonly Utf8String[] topics;

    /// <summary>Where each topic's partitions begin, so a record's entry is found without scanning.</summary>
    private readonly int[] entryStart;

    private FetchEntry[] entries = [];
    private FetchResponse? response;

    public ToyStreamConsumer(IClientTransport transport, IEnumerable<string> topics, ConsumerOptions? options = null)
    {
        ArgumentNullException.ThrowIfNull(topics);

        this.topics = topics.Select(name => new Utf8String(name)).ToArray();
        ArgumentOutOfRangeException.ThrowIfZero(this.topics.Length);
        ArgumentOutOfRangeException.ThrowIfGreaterThan(this.topics.Length, ProtocolConstants.MaxTopicsPerFetch);

        this.connection = new ClientConnection<ClientHandler>(transport, ConnectionRole.Consumer, default);
        this.options = options ?? new ConsumerOptions();
        this.entryStart = new int[this.topics.Length];
    }

    /// <summary>
    /// Connects and resolves every topic's partitions. Expected to be awaited up front so a missing
    /// topic surfaces here, but a read still does it on demand.
    /// </summary>
    public async ValueTask ConnectAsync(CancellationToken cancellationToken = default) =>
        await EnsureStarted(cancellationToken).ConfigureAwait(false);

    /// <summary>
    /// Advances to the next record, fetching when the current response runs out. False means the
    /// server had nothing after waiting, not that the stream has ended.
    /// </summary>
    public async ValueTask<bool> MoveNextAsync(CancellationToken cancellationToken = default)
    {
        var connected = await EnsureStarted(cancellationToken).ConfigureAwait(false);

        if (this.response is { } open)
        {
            if (await open.MoveNextAsync(cancellationToken).ConfigureAwait(false))
            {
                Advance(open);
                return true;
            }

            // A batch boundary is the server's business, so a spent response is refilled rather
            // than reported.
            await open.DisposeAsync().ConfigureAwait(false);
            this.response = null;
        }

        var fetched = await SendFetch(connected, cancellationToken).ConfigureAwait(false);
        if (fetched.Result != FetchResult.Success)
        {
            await fetched.DisposeAsync().ConfigureAwait(false);
            ThrowFetchFailed(fetched.Result);
        }

        if (await fetched.MoveNextAsync(cancellationToken).ConfigureAwait(false))
        {
            this.response = fetched;
            Advance(fetched);
            return true;
        }

        await fetched.DisposeAsync().ConfigureAwait(false);
        return false;
    }

    public FetchedRecordHeader Current => Open().Current;

    public Utf8String Topic => this.topics[Open().Current.TopicIndex];

    public ReadOnlyMemory<byte> Key => Open().Key;

    /// <summary>
    /// Streams the current record's body out. Unbounded, and valid only until the next move, so a
    /// caller that wants to keep it copies it here.
    /// </summary>
    public ValueTask CopyBodyTo(PipeWriter destination, CancellationToken cancellationToken = default) =>
        Open().CopyBodyTo(destination, cancellationToken);

    public async ValueTask DisposeAsync()
    {
        if (this.response is { } open)
        {
            this.response = null;
            await open.DisposeAsync().ConfigureAwait(false);
        }

        await this.connection.DisposeAsync().ConfigureAwait(false);
    }

    private async ValueTask<ConnectionBase> EnsureStarted(CancellationToken cancellationToken)
    {
        var connected = await this.connection.EnsureConnected(cancellationToken).ConfigureAwait(false);
        if (this.entries.Length > 0)
        {
            return connected;
        }

        var resolved = new List<FetchEntry>();
        for (var i = 0; i < this.topics.Length; i++)
        {
            this.entryStart[i] = resolved.Count;

            var described = await Describe(connected, this.topics[i], cancellationToken).ConfigureAwait(false);
            if (described.Result != DescribeTopicResult.Success)
            {
                ThrowUnknownTopic(this.topics[i]);
            }

            // Indexed inline: a ref struct cannot be a local in an async method.
            for (var partition = 0; partition < described.Partitions.Length; partition++)
            {
                resolved.Add(new FetchEntry((short)i, (short)partition,
                    this.options.StartAt == StartPosition.Latest
                        ? described.Partitions[partition].EndOffset
                        : described.Partitions[partition].StartOffset));
            }
        }

        if (resolved.Count > ProtocolConstants.MaxFetchEntries)
        {
            ThrowTooManyPartitions(resolved.Count);
        }

        this.entries = resolved.ToArray();
        return connected;
    }

    /// <summary>The next fetch resumes past what was just handed out, so nothing is read twice.</summary>
    private void Advance(FetchResponse open)
    {
        var record = open.Current;
        this.entries[this.entryStart[record.TopicIndex] + record.PartitionId] =
            new FetchEntry(record.TopicIndex, record.PartitionId, record.Offset + 1);
    }

    // Split out because a ref struct message cannot be built inside an async method.
    private ValueTask<FetchResponse> SendFetch(ConnectionBase connected, CancellationToken cancellationToken) =>
        connected.SendMessageForStreamedResponse<FetchRequestMessage, FetchResponseMessage, FetchResponse>(
            new FetchRequestMessage
            {
                MaxWaitMilliseconds = this.options.MaxWaitMilliseconds,
                MaxBytes = this.options.MaxBytes,
                Topics = new TopicNames(this.topics),
                Entries = new FetchEntries(this.entries)
            }, cancellationToken);

    private static ValueTask<DescribeTopicResponse> Describe(ConnectionBase connected, Utf8String topic,
        CancellationToken cancellationToken) =>
        connected.SendMessage<DescribeTopicRequestMessage, DescribeTopicResponseMessage, DescribeTopicResponse>(
            new DescribeTopicRequestMessage { TopicName = topic }, cancellationToken);

    private FetchResponse Open() => this.response ?? ThrowNoCurrentRecord();

    [DoesNotReturn]
    [StackTraceHidden]
    private static FetchResponse ThrowNoCurrentRecord() =>
        throw new InvalidOperationException("There is no record to read; call MoveNextAsync first.");

    [DoesNotReturn]
    [StackTraceHidden]
    private static void ThrowUnknownTopic(Utf8String topic) =>
        throw new InvalidOperationException($"Topic '{(string)topic}' does not exist.");

    [DoesNotReturn]
    [StackTraceHidden]
    private static void ThrowFetchFailed(FetchResult result) =>
        throw new InvalidOperationException($"Fetch failed with {result}.");

    [DoesNotReturn]
    [StackTraceHidden]
    private static void ThrowTooManyPartitions(int count) =>
        throw new InvalidOperationException(
            $"The requested topics have {count} partitions, more than the {ProtocolConstants.MaxFetchEntries} one fetch may cover.");
}
