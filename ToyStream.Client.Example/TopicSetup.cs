namespace ToyStream.Client.Example;

using Microsoft.Extensions.Options;
using ToyStream.Protocol.Responses;

/// <summary>
/// Creates every topic the workers use before any of them start. An <see cref="IHostedService"/>
/// rather than a <see cref="BackgroundService"/> on purpose: startup waits for this one to finish,
/// which is what keeps the readers from racing the topics into existence.
/// </summary>
public class TopicSetup(
    IClientTransport transport,
    IOptions<ProducerOptions> producer,
    IOptions<ConsumerOptions> consumer,
    IOptions<SubscriberOptions> subscriber,
    ILogger<TopicSetup> logger) : IHostedService
{
    private const short PartitionCount = 1;

    public async Task StartAsync(CancellationToken cancellationToken)
    {
        var topics = consumer.Value.Topics
            .Append(producer.Value.Topic)
            .Append(subscriber.Value.Topic)
            .Where(topic => !string.IsNullOrEmpty(topic))
            .Distinct(StringComparer.Ordinal);

        await using var admin = new ToyStreamAdminClient(transport);

        foreach (var topic in topics)
        {
            var result = await admin.CreateTopic(topic, PartitionCount, cancellationToken);

            // Already there is the normal case on every run after the first.
            if (result is not (CreateTopicResult.Success or CreateTopicResult.DuplicateTopic))
            {
                throw new InvalidOperationException($"Could not create topic '{topic}': {result}.");
            }

            logger.LogInformation("Topic {Topic} is ready ({Result})", topic, result);
        }
    }

    public Task StopAsync(CancellationToken cancellationToken) => Task.CompletedTask;
}
