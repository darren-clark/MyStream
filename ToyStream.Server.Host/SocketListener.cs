namespace ToyStream.Server.Host;

using System.Net;
using System.Net.Sockets;

public class SocketListener(ILogger<SocketListener> logger)
{
    public async Task Listen(int port, Action<Socket> accepted, CancellationToken cancellationToken)
    {
        // Dual mode rather than IPAddress.Any: on Windows "localhost" resolves to ::1 first, which
        // an IPv4-only listener refuses.
        using var listener = new TcpListener(IPAddress.IPv6Any, port);
        listener.Server.DualMode = true;
        listener.Start();
        logger.LogInformation("Listening on port {port}", port);
        
        while (!cancellationToken.IsCancellationRequested)
        {
            var client = await listener.AcceptSocketAsync(cancellationToken);
            accepted(client);
        }
    }
}