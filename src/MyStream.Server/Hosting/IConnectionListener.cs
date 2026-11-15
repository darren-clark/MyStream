using ToyStream.Protocol;

namespace MyStream.Server.Hosting;

public interface IConnectionListener
{
    Task Listen(Action<ITransportConnection> accept, CancellationToken cancellationToken);   
}