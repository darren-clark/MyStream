namespace ToyStream.Client.Example;

/// <summary>Pulls records with fetches and logs them.</summary>
public class ConsumerWorker(IToyStreamConsumer consumer, ILogger<ConsumerWorker> logger) : BackgroundService
{
    protected override async Task ExecuteAsync(CancellationToken stoppingToken)
    {
        await consumer.ConnectAsync(stoppingToken);

        while (!stoppingToken.IsCancellationRequested)
        {
            // False is an empty answer after the server's wait, not the end of anything, so the
            // loop asks again rather than stopping.
            if (!await consumer.MoveNextAsync(stoppingToken))
            {
                continue;
            }

            var record = consumer.Current;
            var topic = consumer.Topic;
            var key = consumer.Key.AsText();
            var body = await RecordBody.ReadAsString(consumer.CopyBodyTo, stoppingToken);

            logger.LogInformation("Fetched {Topic}/{Partition}@{Offset} key={Key} body={Body}",
                (string)topic, record.PartitionId, record.Offset, key, body);
        }
    }
}