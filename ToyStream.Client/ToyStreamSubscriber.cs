namespace ToyStream.Client;

using System.IO.Pipelines;
using ToyStream.Protocol;
using ToyStream.Protocol.Messages;
using ToyStream.Shared;

public sealed record SubscriberOptions
{
    public SubscribeOffset StartAt { get; init; } = SubscribeOffset.Earliest;
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
public sealed class ToyStreamSubscriber : IAsyncDisposable
{
    private readonly SubscriptionStream stream = new();
    private readonly ClientConnection<SubscriberClientHandler> connection;
    private readonly SubscriberOptions options;
    private readonly Utf8String topic;

    private Task? watching;
    private bool subscribed;

    public ToyStreamSubscriber(IClientTransport transport, string topic, SubscriberOptions? options = null)
    {
        ArgumentException.ThrowIfNullOrEmpty(topic);

        this.topic = new Utf8String(topic);
        this.options = options ?? new SubscriberOptions();
        this.connection = new ClientConnection<SubscriberClientHandler>(
            transport, ConnectionRole.Subscriber, new SubscriberClientHandler(this.stream));
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
        return await this.stream.MoveNextAsync(cancellationToken).ConfigureAwait(false);
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
        // Before the connection goes: the read loop may be parked inside a handler holding a batch,
        // and would otherwise never return to notice the cancellation.
        this.stream.Release();
        this.stream.Close();

        await this.connection.DisposeAsync().ConfigureAwait(false);

        if (this.watching is { } watch)
        {
            await watch.ConfigureAwait(false);
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
        await SendSubscribe(connected, this.topic, this.options.StartAt, cancellationToken).ConfigureAwait(false);
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
    private static ValueTask SendSubscribe(ConnectionBase connection, Utf8String topic, SubscribeOffset startAt,
        CancellationToken cancellationToken) =>
        connection.SendMessage(new SubscribeMessage { Topic = topic, StartOffset = startAt }, cancellationToken);
}
