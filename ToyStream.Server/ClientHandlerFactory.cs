namespace ToyStream.Server.Host;

using System.IO.Pipelines;
using System.Net.Sockets;
using Protocol;

public class ClientHandlerFactory(IServerCore server)
{
    public AdminClientConnection CreateClientHandler(Socket socket, CancellationToken cancellationToken)
   
        => new AdminClientConnection(server, new NetworkStream(socket, ownsSocket: true), cancellationToken);
}