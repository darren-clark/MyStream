namespace ToyStream.Server;

using System.Buffers;
using ToyStream.Protocol;
using ToyStream.Protocol.Messages;
using ToyStream.Protocol.Responses;

/// <summary>
/// Adapts a partition to the protocol's payload sink. One per connection, reused across messages, so
/// streaming a payload allocates nothing per message.
/// </summary>
internal sealed class PublishEventReceiver : IPayloadReceiver
{
    private IPartitionWriter? partition;
    private PublishResult failure;

    /// <summary>
    /// Resolution happens while the header is still in the buffer, because the topic name is a view
    /// over bytes the connection advances past as soon as streaming begins.
    /// </summary>
    public PublishEventReceiver Begin(IPartitionWriter? target, PublishResult failureResult)
    {
        this.partition = target;
        this.failure = failureResult;
        return this;
    }

    public ValueTask ReceiveAsync(ReadOnlySequence<byte> chunk, CancellationToken cancellationToken)
        // A payload for an unknown topic is still drained, or the next message would be read from
        // the middle of this one.
        => this.partition?.AppendAsync(chunk, cancellationToken) ?? default;

    public ValueTask CompleteAsync(PrimitiveWriter writer, CancellationToken cancellationToken)
    {
        if (this.partition is null)
        {
            WriteResult(writer, this.failure);
            return default;
        }

        var commit = this.partition.CommitAsync(cancellationToken);
        this.partition = null;

        if (commit.IsCompletedSuccessfully)
        {
            WriteResult(writer, commit.Result);
            return default;
        }

        return AwaitCommit(commit, writer);

        static async ValueTask AwaitCommit(ValueTask<PublishResult> pending, PrimitiveWriter writer)
            => WriteResult(writer, await pending.ConfigureAwait(false));
    }

    private static void WriteResult(PrimitiveWriter writer, PublishResult result)
        => new ResponseWriter<AcknowledgeMessage<PublishResult>>(writer)
            .WriteResponse(AcknowledgeMessage.WithResult(result));
}
