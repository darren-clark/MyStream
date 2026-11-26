using System.Buffers;
using System.IO.Pipelines;

namespace MyStream.Server.Topics;

public readonly struct FetchedEvent
{
    public PipeReader Key { get; init; }
    public PipeReader Body { get; init; }
}
