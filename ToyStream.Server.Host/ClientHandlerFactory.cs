namespace ToyStream.Server.Host;

using System.Net.Sockets;

public class ClientHandlerFactory(IServerCore server)
{
    public ClientConnection CreateClientHandler(Socket socket, CancellationToken cancellationToken)
        => new ClientConnection(server, new NetworkStream(socket, ownsSocket: true), cancellationToken);
}