namespace ToyStream.Server;

using System.Net.Sockets;
using Microsoft.Extensions.Options;

public class Worker(ILogger<Worker> logger, ConnectionManager connectionManager, IOptionsMonitor<ToyStreamServerOptions> options) : BackgroundService
{
    
    
    protected override async Task ExecuteAsync(CancellationToken stoppingToken)
    {
        while (!stoppingToken.IsCancellationRequested)
        {
            if (logger.IsEnabled(LogLevel.Information))
            {
                logger.LogInformation("Worker running at: {time}", DateTimeOffset.Now);
            }

            await new SocketListener(options.CurrentValue.Port, Accept).Listen(stoppingToken);

            
            void Accept(Socket socket)
            {
                
            }
        }
    }
}