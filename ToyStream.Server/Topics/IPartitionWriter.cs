namespace ToyStream.Server;

using System.Buffers;
using ToyStream.Protocol.Responses;

/// <summary>Where a published payload goes. Chunks are pipe memory and must be copied to be kept.</summary>
public interface IPartitionWriter
{
    ValueTask AppendAsync(ReadOnlySequence<byte> chunk, CancellationToken cancellationToken);

    ValueTask<PublishResult> CommitAsync(CancellationToken cancellationToken);
}
