using Microsoft.Extensions.DependencyInjection;
using Microsoft.Extensions.Options;

namespace ToyStream.Client;

public static class DependencyInjectionExtensions
{

    public static IToyStreamClientBuilder AddToyStreamClient(this IServiceCollection services, string configurationSection = "ToyStream")
     => new ToyStreamClientBuilder(services, configurationSection);

    private class ToyStreamClientBuilder(IServiceCollection services, string configurationSection): IToyStreamClientBuilder
    {
        public IServiceCollection Services { get; } = services;
        public string ConfigurationSection { get; } = configurationSection;
    }

    public static IToyStreamClientBuilder WithTcpConnection(this IToyStreamClientBuilder builder,
        Action<TcpTransportOptions> configure)
    {
        builder.Services.Configure(configure);
        builder.Services.AddTransient<IClientTransport>(Transport);
        return builder;
    }
    
    public static IToyStreamClientBuilder WithTcpConnection(this IToyStreamClientBuilder builder,
        string? configurationSection = null )
    {
        builder.Services.AddOptions<TcpTransportOptions>()
            .BindConfiguration(configurationSection ?? builder.ConfigurationSection);

        builder.Services.AddTransient<IClientTransport>(Transport);
        return builder;
    }

    public static IToyStreamClientBuilder AddConsumer(this IToyStreamClientBuilder builder,
        string? configurationSection = null)
    {
        builder.Services.AddOptions<ConsumerOptions>()
            .BindConfiguration(configurationSection ?? builder.ConfigurationSection);
        
        builder.Services.AddTransient<IToyStreamConsumer>(Consumer);
        return builder;
    }

    public static IToyStreamClientBuilder AddConsumer(this IToyStreamClientBuilder builder,
        Action<ConsumerOptions> configure)
    {
        builder.Services.Configure(configure);
        builder.Services.AddTransient<IToyStreamConsumer>(Consumer);
        return builder;
    }

    public static IToyStreamClientBuilder AddProducer(this IToyStreamClientBuilder builder,
        string? configurationSection = null)
    {
        builder.Services.AddOptions<ProducerOptions>()
            .BindConfiguration(configurationSection ?? builder.ConfigurationSection);
        
        builder.Services.AddTransient<IToyStreamProducer>(Producer);
        return builder;
    }

    public static IToyStreamClientBuilder AddProducer(this IToyStreamClientBuilder builder,
        Action<ProducerOptions> configure)
    {
        builder.Services.Configure(configure);
        builder.Services.AddTransient<IToyStreamProducer>(Producer);
        return builder;
    }

    public static IToyStreamClientBuilder AddSubscriber(this IToyStreamClientBuilder builder,
        string? configurationSection = null)
    {
        
        builder.Services.AddOptions<SubscriberOptions>()
            .BindConfiguration(configurationSection ?? builder.ConfigurationSection);
        builder.Services.AddTransient<IToyStreamSubscriber>(Subscriber);
        return builder;
    }

    public static IToyStreamClientBuilder AddSubscriber(this IToyStreamClientBuilder builder,
        Action<SubscriberOptions> configure)
    {
        builder.Services.Configure(configure);
        builder.Services.AddTransient<IToyStreamSubscriber>(Subscriber);
        return builder;
    }

    // Resolved through a factory rather than by constructor injection, so the clients keep taking a
    // plain options record and never reference the options abstractions themselves.
    private static TcpClientTransport Transport(IServiceProvider services) =>
        new(services.GetRequiredService<IOptions<TcpTransportOptions>>().Value);

    private static ToyStreamConsumer Consumer(IServiceProvider services) =>
        new(services.GetRequiredService<IClientTransport>(),
            services.GetRequiredService<IOptions<ConsumerOptions>>().Value);

    private static ToyStreamProducer Producer(IServiceProvider services) =>
        new(services.GetRequiredService<IClientTransport>(),
            services.GetRequiredService<IOptions<ProducerOptions>>().Value);

    private static ToyStreamSubscriber Subscriber(IServiceProvider services) =>
        new(services.GetRequiredService<IClientTransport>(),
            services.GetRequiredService<IOptions<SubscriberOptions>>().Value);
}

public interface IToyStreamClientBuilder
{
    internal IServiceCollection Services { get; }
    internal string ConfigurationSection { get; }
}