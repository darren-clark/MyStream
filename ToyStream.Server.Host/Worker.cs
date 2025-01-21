namespace ToyStream.Server.Host;

using System.Net.Sockets;
using Microsoft.Extensions.Options;

public class Worker(ILogger<Worker> logger, ClientHandlerFactory clientHandlerFactory, IOptionsMonitor<ToyStreamServerOptions> options, SocketListener socketListener) : BackgroundService
{
    
    protected override async Task ExecuteAsync(CancellationToken stoppingToken)
    {
        logger.LogInformation("Worker start running at: {time}", DateTimeOffset.Now);

        await socketListener.Listen(options.CurrentValue.Port, Accept, stoppingToken);


        void Accept(Socket socket) => clientHandlerFactory.CreateClientHandler(socket, stoppingToken);
    }
}