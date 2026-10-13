using System.IO.Pipelines;

namespace ToyStream.Protocol.Messages;

/// <summary>
/// A streamed request: the header that describes it, plus the body that follows it on the wire.
/// </summary>
/// <remarks>
/// The body is not part of the header's encoding, so it is carried here rather than on the message -
/// which also lets one type describe a message being sent and one being received.
/// </remarks>
public readonly ref struct StreamingMessage<TMessage>
    where TMessage : struct, IStreamingRequestMessage, allows ref struct
{
    public required TMessage Header { get; init; }

    /// <summary>
    /// Total body length when known, letting the receiver size its buffer up front; null streams it
    /// as it arrives. Advisory only - the chunk framing decides where the body ends.
    /// </summary>
    public required long? Size { get; init; }

    public required PipeReader Body { get; init; }
}
