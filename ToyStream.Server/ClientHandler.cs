namespace ToyStream.Server;

using System.Net.Sockets;

public class ClientHandler(IServerCore core, Socket socket, CancellationToken token): IAdminClient
{
    
}