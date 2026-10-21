using System.IO.Pipelines;
using ToyStream.Protocol.Responses;

namespace ToyStream.Server;

// Has to be separate from initial sync startEventWrite
// since that takes ref struct args.
public interface IEventWriter
{
    ValueTask<PublishResult> WriteEvent(PipeReader input, CancellationToken cancellationToken);
}