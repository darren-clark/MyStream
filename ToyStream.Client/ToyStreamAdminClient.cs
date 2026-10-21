namespace ToyStream.Client;

using ToyStream.Protocol;
using ToyStream.Protocol.Messages;
using ToyStream.Protocol.Responses;

/// <summary>
/// Topic administration. Separate from the producer because its connection is typically short
/// lived - create the topics, then dispose - while a producer's outlives the whole process.
/// </summary>
/// <remarks>Not thread safe: the underlying connection allows one outstanding request.</remarks>
public sealed class ToyStreamAdminClient(IClientTransport transport) : IAsyncDisposable
{
    private readonly ClientConnection connection = new(transport);

    /// <summary>
    /// Expected to be awaited before the first request, so an unreachable host fails here rather
    /// than inside request handling. Idempotent, and requests still connect on demand without it.
    /// </summary>
    public async ValueTask ConnectAsync(CancellationToken cancellationToken = default) =>
        await this.connection.EnsureConnected(cancellationToken).ConfigureAwait(false);

    public async ValueTask<CreateTopicResult> CreateTopic(string topicName, short partitionCount,
        CancellationToken cancellationToken = default)
    {
        var connected = await this.connection.EnsureConnected(cancellationToken).ConfigureAwait(false);
        return await SendCreateTopic(connected, topicName, partitionCount, cancellationToken).ConfigureAwait(false);
    }

    // Split out because a ref struct message cannot be built inside an async method.
    private static ValueTask<CreateTopicResult> SendCreateTopic(ConnectionBase connectionBase, string topicName,
        short partitionCount, CancellationToken cancellationToken) =>
        connectionBase.WithResult<CreateTopicResult>().Send(new CreateTopicMessage
        {
            TopicName = topicName,
            PartitionCount = partitionCount
        }, cancellationToken);

    public ValueTask DisposeAsync() => this.connection.DisposeAsync();
}
