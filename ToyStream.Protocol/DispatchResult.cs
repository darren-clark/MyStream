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

    public bool IsSuccess { get; }
    public bool WroteResponse { get; }
    public ValueTask Task { get; }

    /// <summary>How much more is needed beyond what the reader already held.</summary>
    public int BytesNeeded { get; }

    public static DispatchResult Insufficient(int bytesNeeded) => new(bytesNeeded);

    public static DispatchResult Dispatched(ValueTask dispatched, bool wroteResponse = true) =>
        new(wroteResponse, dispatched);
}
