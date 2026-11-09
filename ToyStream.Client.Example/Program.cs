using ToyStream.Client;
using ToyStream.Client.Example;

var builder = Host.CreateApplicationBuilder(args);

var services = builder.Services;

services.AddToyStreamClient()
    .WithTcpConnection()
    .AddConsumer("ToyStream:Consumer")
    .AddProducer("ToyStream:Producer")
    .AddSubscriber("ToyStream:Subscriber");

// First, so the topic exists before the readers connect to it.
services.AddHostedService<TopicSetup>();
services.AddHostedService<ProducerWorker>();
services.AddHostedService<ConsumerWorker>();
services.AddHostedService<SubscriberWorker>();

var host = builder.Build();
host.Run();