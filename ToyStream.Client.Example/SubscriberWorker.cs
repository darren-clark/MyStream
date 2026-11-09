namespace ToyStream.Client.Example;

using Microsoft.Extensions.Options;

/// <summary>Logs records the server pushes, without ever asking for them.</summary>
public class SubscriberWorker(
    IToyStreamSubscriber subscriber,
    IOptions<SubscriberOptions> options,
    ILogger<SubscriberWorker> logger) : BackgroundService
{
    protected override async Task ExecuteAsync(CancellationToken stoppingToken)
    {
        try
        {
            await subscriber.ConnectAsync(stoppingToken);
            logger.LogInformation("Subscribed to {Topic} from {StartAt}",
                options.Value.Topic, options.Value.StartAt);

            // Unlike a fetch, false here means the subscription itself has ended.
            while (await subscriber.MoveNextAsync(stoppingToken))
            {
                var record = subscriber.Current;
                var key = subscriber.Key.AsText();
                var body = await RecordBody.ReadAsString(subscriber.CopyBodyTo, stoppingToken);

                logger.LogInformation("Pushed {Partition}@{Offset} key={Key} body={Body}",
                    record.PartitionId, record.Offset, key, body);
            }

            logger.LogInformation("The subscription ended.");
        }
        catch (SubscriptionLostException lost)
        {
            // The server has no response to a subscribe, so a refusal arrives this way instead.
            logger.LogError(lost, "The server dropped the subscription: {Reason}", lost.Reason);
        }
        catch (OperationCanceledException) when (stoppingToken.IsCancellationRequested)
        {
        }
    }
}