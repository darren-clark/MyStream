namespace ToyStream.Protocol;

/// <summary>
/// Carries a dispatched handler's <see cref="ValueTask"/> back to the read loop, which is the only
/// async method on the per-message path.
/// </summary>
public readonly struct DispatchResult
{
    private DispatchResult(bool wroteResponse, ValueTask task)
    {
        this.IsSuccess = true;
        this.WroteResponse = wroteResponse;
        this.Task = task;
    }

    private DispatchResult(int bytesNeeded) => this.BytesNeeded = bytesNeeded;

    private DispatchResult(IPayloadReceiver receiver, long declaredBodySize)
    {
        this.IsSuccess = true;
        this.Receiver = receiver;
        this.DeclaredBodySize = declaredBodySize;
    }

    public bool IsSuccess { get; }
    public bool WroteResponse { get; }
    public ValueTask Task { get; }

    /// <summary>How much more is needed beyond what the reader already held.</summary>
    public int BytesNeeded { get; }

    /// <summary>Non-null when the connection should pump a chunked body into it before reading further.</summary>
    public IPayloadReceiver? Receiver { get; }

    /// <summary>Body length the header declared, or negative when the sender did not know it.</summary>
    public long DeclaredBodySize { get; }

    public static DispatchResult Insufficient(int bytesNeeded) => new(bytesNeeded);

    public static DispatchResult Dispatched(ValueTask dispatched, bool wroteResponse = true) =>
        new(wroteResponse, dispatched);

    /// <summary>
    /// The header is parsed and a chunked body follows. The handler consumes nothing itself; the
    /// connection feeds the receiver and writes the response through it.
    /// </summary>
    public static DispatchResult Streaming(IPayloadReceiver receiver, long? declaredBodySize = null) =>
        new(receiver, declaredBodySize ?? -1);
}
