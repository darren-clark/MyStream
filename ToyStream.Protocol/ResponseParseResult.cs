namespace ToyStream.Protocol;

using System.IO.Pipelines;

/// <summary>What a parsed response left for the read loop to finish.</summary>
internal readonly struct ResponseParseResult
{
    private ResponseParseResult(bool isParsed, int bytesNeeded, bool ownsBody, ValueTask bodyDrained)
    {
        IsParsed = isParsed;
        BytesNeeded = bytesNeeded;
        OwnsBody = ownsBody;
        BodyDrained = bodyDrained;
    }

    public bool IsParsed { get; }

    /// <summary>How much more is needed beyond what the reader already held.</summary>
    public int BytesNeeded { get; }

    /// <summary>
    /// Set when the awaiting caller was handed the body. The connection's buffer is then the body
    /// reader's, so the read loop must not touch it until <see cref="BodyDrained"/> completes.
    /// </summary>
    public bool OwnsBody { get; }

    public ValueTask BodyDrained { get; }

    public static ResponseParseResult Insufficient(int bytesNeeded) => new(false, bytesNeeded, false, default);

    public static ResponseParseResult Completed() => new(true, 0, false, default);

    public static ResponseParseResult Streaming(ValueTask bodyDrained) => new(true, 0, true, bodyDrained);
}

/// <summary>
/// A response whose body is streamed rather than parsed from the buffer. The awaiting caller reads
/// it, and the connection cannot continue until it says it is finished.
/// </summary>
public interface IStreamingResponse
{
    ValueTask BodyDrained { get; }
}

/// <summary>
/// The streamed counterpart of <see cref="IResponse{TResponseMessage,TResponse}"/>: built from the
/// header while the buffer is valid, then given the body to read.
/// </summary>
public interface IStreamingResponse<TResponseMessage, out TResponse> : IStreamingResponse
    where TResponseMessage : struct, IResponseMessage<TResponseMessage>, allows ref struct
    where TResponse : IStreamingResponse<TResponseMessage, TResponse>
{
    internal static abstract TResponse CreateResponse(ref readonly TResponseMessage header, PipeReader body);
}
