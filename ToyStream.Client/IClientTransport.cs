namespace ToyStream.Client;

using System.IO.Pipelines;

/// <summary>
/// A connected duplex byte stream together with whatever owns it, so the client can tear the
/// transport down without knowing whether it is a socket, a loopback pipe or a test double.
/// </summary>
public interface ITransportConnection : IDuplexPipe, IAsyncDisposable;

/// <summary>
/// Connecting is asynchronous and a constructor is not, which is why the client composes a transport
/// rather than deriving from something that demands a live pipe pair up front.
/// </summary>
public interface IClientTransport
{
    ValueTask<ITransportConnection> ConnectAsync(CancellationToken cancellationToken);
}
