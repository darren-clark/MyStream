namespace ToyStream.Client;

using System.IO.Pipelines;
using ToyStream.Protocol;
using ToyStream.Protocol.Messages;
using ToyStream.Protocol.Responses;
using ToyStream.Shared;

/// <summary>
/// Publishes event bodies. Holds its own connection because a streamed publish occupies the
/// connection for the whole body, which would otherwise reject any concurrent admin request.
/// </summary>
/// <remarks>Not thread safe: the underlying connection allows one outstanding request.</remarks>
public sealed class ToyStreamProducer(IClientTransport transport) : IAsyncDisposable
{
    private readonly ClientConnection<ClientHandler> connection = new(transport, ConnectionRole.Producer, default);

    /// <summary>
    /// Expected to be awaited before the first publish, so an unreachable host fails here rather
    /// than inside request handling. Idempotent, and publishes still connect on demand without it.
    /// </summary>
    public async ValueTask ConnectAsync(CancellationToken cancellationToken = default) =>
        await this.connection.EnsureConnected(cancellationToken).ConfigureAwait(false);

    /// <param name="size">Body length when known, letting the server size its buffer; null streams it.</param>
    public async ValueTask<PublishResult> Publish(string topic, short partition, PipeReader body,
        long? size = null, CancellationToken cancellationToken = default)
    {
        var connected = await this.connection.EnsureConnected(cancellationToken).ConfigureAwait(false);
        return await SendPublish(connected, topic, partition, default, body, size, cancellationToken)
            .ConfigureAwait(false);
    }

    /// <summary>
    /// The key is carried for the consumer, not used for routing: the partition is chosen here, so
    /// the server never has to hash it.
    /// </summary>
    public async ValueTask<PublishResult> Publish(string topic, short partition, ReadOnlyMemory<byte> key,
        PipeReader body, long? size = null, CancellationToken cancellationToken = default)
    {
        var connected = await this.connection.EnsureConnected(cancellationToken).ConfigureAwait(false);
        return await SendPublish(connected, topic, partition, key, body, size, cancellationToken)
            .ConfigureAwait(false);
    }

    // Split out because a ref struct message cannot be built inside an async method.
    private static ValueTask<PublishResult> SendPublish(ConnectionBase connectionBase, string topic, short partition,
        ReadOnlyMemory<byte> key, PipeReader body, long? size, CancellationToken cancellationToken) =>
        connectionBase.WithResult<PublishResult>().SendStreaming(new StreamingMessage<PublishMessage>
        {
            Header = new PublishMessage
            {
                Topic = topic,
                Partition = partition,
                Key = new RefBytes(key.Span)
            },
            Size = size,
            Body = body
        }, cancellationToken);

    public ValueTask DisposeAsync() => this.connection.DisposeAsync();
}
