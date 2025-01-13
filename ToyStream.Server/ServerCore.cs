namespace ToyStream.Server.Host;

using System.Collections.Concurrent;

public interface IServerCore
{
    public void RegisterClient(IAdminClient client);
    public void UnregisterClient(IAdminClient client);
}

public class ServerCore: IServerCore, IAsyncDisposable
{
    private readonly ITopicManager topicManager;
    private readonly ConcurrentDictionary<string, IAdminClient> clients = new();

    public ServerCore(ITopicManager topicManager)
    {
        this.topicManager = topicManager;
    }

    public void RegisterClient(IAdminClient client)
    {
        _ = this.clients.TryAdd(client.ClientId, client) && ThrowDuplicateClientException(client.ClientId);
    }

    public void UnregisterClient(IAdminClient client)
    {
        _ = this.clients.TryRemove(client.ClientId, out _) && ThrowClientNotFoundException(client.ClientId);
    }

    private static bool ThrowDuplicateClientException(string clientId)
    {
        throw new InvalidOperationException($"Client with id {clientId} already exists.");
    }
    
    private static bool ThrowClientNotFoundException(string clientId)
    {
        throw new InvalidOperationException($"Client with id {clientId} does not exist.");
    }
    
    public async ValueTask DisposeAsync()
    {
        await Task.WhenAll(this.clients.Values.Select(async client => await client.DisposeAsync()));
    }
}