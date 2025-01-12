namespace ToyStream.Server;

using System.Collections.Concurrent;

public interface IServerCore
{
    public void RegisterClient(IAdminClient client);
}

public class ServerCore: IServerCore
{
    private readonly ConcurrentBag<IAdminClient> clients = new();
        
    public void RegisterClient(IAdminClient client)
    {
        this.clients.Add(client);
    }
}