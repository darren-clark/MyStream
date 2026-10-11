namespace ToyStream.Server.Host;

using System.IO.Pipelines;
using System.Net.Sockets;
using Protocol;

public class ClientHandlerFactory(ITopicService topics)
{
    public Connection<ServerHandler> CreateClientHandler(Socket socket, CancellationToken cancellationToken)
    {
        var stream = new NetworkStream(socket, ownsSocket: true);
        var connection = new Connection<ServerHandler>(
            PipeReader.Create(stream), PipeWriter.Create(stream), new ServerHandler(topics));

        // Start returns only when the read loop ends, so it is deliberately not awaited here.
        _ = connection.Start(cancellationToken);
        return connection;
    }
}