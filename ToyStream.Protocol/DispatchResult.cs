namespace ToyStream.Protocol;

/// <summary>
/// Carries a dispatched handler's <see cref="ValueTask"/> back to the read loop, which is the only
/// async method on the per-message path.
/// </summary>
public readonly struct DispatchResult
{
    private DispatchResult(ValueTask task, bool wroteResponse, bool ownsBody)
    {
        this.IsSuccess = true;
        this.WroteResponse = wroteResponse;
        this.Task = task;
        this.OwnsBody = ownsBody;
    }

    private DispatchResult(int bytesNeeded) => this.BytesNeeded = bytesNeeded;

    public bool IsSuccess { get; }
    public bool WroteResponse { get; }
    public ValueTask Task { get; }

    /// <summary>How much more is needed beyond what the reader already held.</summary>
    public int BytesNeeded { get; }

    /// <summary>
    /// Set when the handler was given the message body. The connection's buffer is then the body
    /// reader's, so the read loop must not touch it again until the dispatched task has completed.
    /// </summary>
    public bool OwnsBody { get; }

    /// <summary>Body length the header declared, or negative when the sender did not know it.</summary>

    public static DispatchResult Insufficient(int bytesNeeded) => new(bytesNeeded);

    public static DispatchResult Dispatched(ValueTask dispatched, bool wroteResponse = true) =>
        new(dispatched, wroteResponse, ownsBody: false);

    /// <summary>
    /// The header is parsed and the handler is reading the body that follows it.
    /// </summary>
    public static DispatchResult Streaming(ValueTask dispatched, bool wroteResponse = true) =>
        new(dispatched, wroteResponse, ownsBody: true);
}
