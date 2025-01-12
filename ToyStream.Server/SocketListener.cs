namespace ToyStream.Server;

using System.Net;
using System.Net.Sockets;

public class SocketListener(int port, Action<Socket> accepted)
{
    public async Task Listen(CancellationToken cancellationToken)
    {
        using var listener = new TcpListener(IPAddress.Any, port);
        listener.Start();
        
        while (!cancellationToken.IsCancellationRequested)
        {
            var client = await listener.AcceptSocketAsync(cancellationToken);
            accepted(client);
        }
    }
}