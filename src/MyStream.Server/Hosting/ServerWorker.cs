using System.Collections.Concurrent;

using Microsoft.Extensions.Hosting;
using Microsoft.Extensions.Logging;

using ToyStream.Protocol;

namespace MyStream.Server.Hosting;

public class ServerWorker(ILogger<ServerWorker> logger, IConnectionListener connectionListener, ServerConnectionHandler connectionHandler): BackgroundService
{
    private readonly ConcurrentDictionary<Guid, Task> active = new();

    protected override async Task ExecuteAsync(CancellationToken stoppingToken)
    {
        logger.LogInformation("MyStream server staring");

        try
        {
            await connectionListener.Listen(Accept, stoppingToken);
        }
        finally
        {
            // The snapshot is enough: the listener has stopped, so nothing new can be added.
            await Task.WhenAll(this.active.Values).ConfigureAwait(false);
        }

        return;

        // don't await 
        void Accept(ITransportConnection transport)
        {
             var id = Guid.NewGuid();            
             var connection = Run(id, transport);

            this.active[id] = connection;

            // Registered after the add so a connection that finished first cannot remove it early.
            _ = connection.ContinueWith(
                static (_, state) =>
                {
                    var (worker, id) = ((ServerWorker, Guid))state!;
                    worker.active.TryRemove(id, out var _);
                },
                (this, id),
                // Specifically don't cancel this continuation
                CancellationToken.None);
        }

        async Task Run(Guid id, ITransportConnection transport)
        {
            try
            {
                await connectionHandler.Start(transport, stoppingToken).ConfigureAwait(false);
            }
            catch (OperationCanceledException)
            {
            }
            catch (Exception ex)
            {
                logger.LogError(ex, "Connection {ConnectionId} faulted", id);
            }
        }
    }
}