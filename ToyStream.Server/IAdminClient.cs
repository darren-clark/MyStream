namespace ToyStream.Server.Host;

public interface IAdminClient: IAsyncDisposable
{
    public string ClientId { get; }
}