namespace ToyStream.Server.Host;

using System.Net;
using System.Net.Sockets;

public class SocketListener(ILogger<SocketListener> logger)
{
    public async Task Listen(int port, Action<Socket> accepted, CancellationToken cancellationToken)
    {
        using var listener = new TcpListener(IPAddress.Any, port);
        listener.Start();
        logger.LogInformation("Listening on port {port}", port);
        
        while (!cancellationToken.IsCancellationRequested)
        {
            var client = await listener.AcceptSocketAsync(cancellationToken);
            accepted(client);
        }
    }
}