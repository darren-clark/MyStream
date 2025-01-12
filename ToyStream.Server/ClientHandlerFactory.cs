namespace ToyStream.Server;

using System.Net.Sockets;

public class ClientHandlerFactory(IServerCore server)
{
    public ClientHandler CreateClientHandler(Socket socket, CancellationToken cancellationToken)
        => new ClientHandler(server, socket, cancellationToken);
}