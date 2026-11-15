namespace MyStream.Protocol;

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

