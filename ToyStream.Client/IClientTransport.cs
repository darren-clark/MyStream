namespace ToyStream.Client;

using ToyStream.Protocol;

/// <summary>
/// Connecting is asynchronous and a constructor is not, which is why the client composes a transport
/// rather than deriving from something that demands a live pipe pair up front.
/// </summary>
public interface IClientTransport
{
    ValueTask<ITransportConnection> ConnectAsync(CancellationToken cancellationToken);
}
