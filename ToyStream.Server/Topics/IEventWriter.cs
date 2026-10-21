using System.IO.Pipelines;

namespace ToyStream.Server;

// Has to be separate from initial sync startEventWrite
// since that takes ref struct args.
public interface IEventWriter
{
    /// <summary>Returns nothing because storage has no failure to report that is not an exception.</summary>
    ValueTask WriteEvent(PipeReader input, CancellationToken cancellationToken);
}