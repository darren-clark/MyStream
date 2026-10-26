namespace ToyStream.Server.Host;

using System.Net.Sockets;
using Microsoft.Extensions.Options;

public class Worker(ILogger<Worker> logger, ConnectionAcceptor acceptor, IOptionsMonitor<ToyStreamServerOptions> options, SocketListener socketListener) : BackgroundService
{
    
    protected override async Task ExecuteAsync(CancellationToken stoppingToken)
    {
        logger.LogInformation("Worker start running at: {time}", DateTimeOffset.Now);

        await socketListener.Listen(options.CurrentValue.Port, Accept, stoppingToken);


        void Accept(Socket socket) => _ = Serve(socket);

        // Nothing awaits the accept, so without this a faulted connection dies leaving no trace
        // anywhere - the exception would surface only as an unobserved task.
        async Task Serve(Socket socket)
        {
            // Read before the acceptor takes ownership: it disposes the socket, and a disposed one
            // throws rather than reporting where the connection came from.
            var endpoint = socket.RemoteEndPoint?.ToString();
            try
            {
                await acceptor.Accept(new SocketConnection(socket), stoppingToken);
            }
            catch (OperationCanceledException) when (stoppingToken.IsCancellationRequested)
            {
            }
            catch (Exception exception)
            {
                logger.LogError(exception, "Connection from {endpoint} faulted", endpoint);
            }
        }
    }
}