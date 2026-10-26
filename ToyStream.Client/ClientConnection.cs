namespace ToyStream.Client;

using ToyStream.Protocol;

/// <summary>
/// Lazily opens one transport and runs the read loop over it. Shared by the role clients, which
/// differ in the role they declare, the messages they send, and what they serve in return.
/// </summary>
internal sealed class ClientConnection<THandler>(IClientTransport transport, ConnectionRole role, THandler handler)
    : IAsyncDisposable
    where THandler : struct, IConnectionHandler
{
    private readonly CancellationTokenSource cancellation = new();

    private ITransportConnection? transportConnection;
    private Connection<THandler>? connection;
    private Task? readLoop;
    private bool disposed;

    /// <summary>
    /// Ends when the peer goes away, which on a one-way connection is the only sign anything is
    /// wrong - nothing else would ever fail.
    /// </summary>
    public Task ReadLoop => this.readLoop ?? Task.CompletedTask;

    public async ValueTask<ConnectionBase> EnsureConnected(CancellationToken cancellationToken)
    {
        ObjectDisposedException.ThrowIf(this.disposed, this);

        if (this.connection is { } established)
        {
            return established;
        }

        // Stored before anything else can throw, so a failure here still leaves the transport disposable.
        var opened = await transport.ConnectAsync(cancellationToken).ConfigureAwait(false);
        this.transportConnection = opened;

        // Before the read loop: the server picks its handler from this, so nothing may precede it.
        await ProtocolHandshake.WriteAsync(opened.Output, role, cancellationToken).ConfigureAwait(false);

        var started = new Connection<THandler>(opened.Input, opened.Output, handler);
        this.connection = started;

        // Start returns only when the read loop ends, so it is deliberately not awaited here.
        this.readLoop = started.Start(this.cancellation.Token);

        return started;
    }

    public async ValueTask DisposeAsync()
    {
        if (this.disposed)
        {
            return;
        }

        this.disposed = true;
        await this.cancellation.CancelAsync().ConfigureAwait(false);

        // Drained before the transport goes, so the loop cannot read from a disposed pipe.
        if (this.readLoop is { } loop)
        {
            try
            {
                await loop.ConfigureAwait(false);
            }
            catch (OperationCanceledException)
            {
            }
        }

        if (this.transportConnection is { } opened)
        {
            await opened.DisposeAsync().ConfigureAwait(false);
        }

        this.cancellation.Dispose();
    }
}
