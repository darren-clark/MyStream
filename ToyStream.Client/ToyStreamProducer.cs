namespace ToyStream.Client;

using System.IO.Pipelines;
using ToyStream.Protocol;
using ToyStream.Protocol.Messages;
using ToyStream.Protocol.Responses;
using ToyStream.Shared;

/// <summary>Where <see cref="ToyStreamProducer.Publish(ReadOnlyMemory{byte},PipeReader,long?,CancellationToken)"/> sends, for a producer with one destination.</summary>
public sealed record ProducerOptions
{
    public string Topic { get; init; } = string.Empty;

    public short Partition { get; init; }
}

public interface IToyStreamProducer : IAsyncDisposable
{
    ValueTask ConnectAsync(CancellationToken cancellationToken = default);

    /// <summary>Publishes to the configured topic and partition.</summary>
    ValueTask<PublishResult> Publish(ReadOnlyMemory<byte> key, PipeReader body, long? size = null,
        CancellationToken cancellationToken = default);

    ValueTask<PublishResult> Publish(string topic, short partition, PipeReader body, long? size = null,
        CancellationToken cancellationToken = default);

    ValueTask<PublishResult> Publish(string topic, short partition, ReadOnlyMemory<byte> key, PipeReader body,
        long? size = null, CancellationToken cancellationToken = default);
}

/// <summary>
/// Publishes event bodies. Holds its own connection because a streamed publish occupies the
/// connection for the whole body, which would otherwise reject any concurrent admin request.
/// </summary>
/// <remarks>Not thread safe: the underlying connection allows one outstanding request.</remarks>
public sealed class ToyStreamProducer(IClientTransport transport, ProducerOptions options) : IToyStreamProducer, IAsyncDisposable
{
    private readonly ClientConnection<ClientHandler> connection = new(transport, ConnectionRole.Producer, default);

    /// <summary>
    /// Expected to be awaited before the first publish, so an unreachable host fails here rather
    /// than inside request handling. Idempotent, and publishes still connect on demand without it.
    /// </summary>
    public async ValueTask ConnectAsync(CancellationToken cancellationToken = default) =>
        await this.connection.EnsureConnected(cancellationToken).ConfigureAwait(false);

    /// <summary>Publishes to the destination in <see cref="ProducerOptions"/>.</summary>
    public async ValueTask<PublishResult> Publish(ReadOnlyMemory<byte> key, PipeReader body,
        long? size = null, CancellationToken cancellationToken = default)
    {
        ArgumentException.ThrowIfNullOrEmpty(options.Topic);

        var connected = await this.connection.EnsureConnected(cancellationToken).ConfigureAwait(false);
        return await SendPublish(connected, options.Topic, options.Partition, key, body, size, cancellationToken)
            .ConfigureAwait(false);
    }

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
