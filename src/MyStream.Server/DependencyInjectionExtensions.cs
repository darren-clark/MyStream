using Microsoft.Extensions.DependencyInjection;

using MyStream.Server.Hosting;

namespace MyStream.Server;

public static class DependencyInjectionExtensions
{
    public interface IMyStreamServerBuilder
    {
        internal IServiceCollection Services { get; }
        string ConfigSection { get; }
    }
    
    public static IMyStreamServerBuilder AddMyStreamServer(this IServiceCollection serviceCollection, string configSection = "MyStream")
    {
        return new MyStreamServerBuilder(serviceCollection, configSection);
    }

    private class MyStreamServerBuilder : IMyStreamServerBuilder
    {
        public MyStreamServerBuilder(IServiceCollection services, string configSection)
        {
            Services = services;
            ConfigSection = configSection;
        }

        public IServiceCollection Services { get; }
        public string ConfigSection { get; }
    }
    
    public static IMyStreamServerBuilder WithTcpListener(this IMyStreamServerBuilder builder, string configSection = "Listener")
    {
        builder.Services.AddOptions<ServerTcpOptions>()
            .BindConfiguration($"{builder.ConfigSection}:{configSection}");
        builder.Services.AddHostedService<ServerWorker>();
        return builder;
    }
}