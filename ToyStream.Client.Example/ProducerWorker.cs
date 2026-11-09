namespace ToyStream.Client.Example;

using Microsoft.Extensions.Options;
using System.IO.Pipelines;
using System.Text;
using ToyStream.Protocol.Responses;

/// <summary>Publishes a record a second to the configured topic, so the readers have something to read.</summary>
public class ProducerWorker(
    IToyStreamProducer producer,
    IOptions<ProducerOptions> options,
    ILogger<ProducerWorker> logger) : BackgroundService
{
    private static readonly TimeSpan Interval = TimeSpan.FromSeconds(1);

    protected override async Task ExecuteAsync(CancellationToken stoppingToken)
    {
        await producer.ConnectAsync(stoppingToken);
        logger.LogInformation("Publishing to {Topic}/{Partition} every {Interval}",
            options.Value.Topic, options.Value.Partition, Interval);

        using var timer = new PeriodicTimer(Interval);
        var sequence = 0;

        while (await timer.WaitForNextTickAsync(stoppingToken))
        {
            var key = Encoding.UTF8.GetBytes($"key-{sequence}");
            var body = Encoding.UTF8.GetBytes($"message {sequence} sent at {DateTimeOffset.Now:O}");

            var result = await Publish(key, body, stoppingToken);
            if (result != PublishResult.Success)
            {
                logger.LogWarning("Publish of {Sequence} was refused with {Result}", sequence, result);
            }

            sequence++;
        }
    }

    private async ValueTask<PublishResult> Publish(byte[] key, byte[] body, CancellationToken cancellationToken)
    {
        // The body travels as a stream, so even an in-memory one is handed over as a reader.
        var payload = new Pipe();
        await payload.Writer.WriteAsync(body, cancellationToken);
        await payload.Writer.CompleteAsync();

        return await producer.Publish(key, payload.Reader, body.Length, cancellationToken);
    }
}