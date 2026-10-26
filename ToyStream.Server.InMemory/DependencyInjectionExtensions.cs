using Microsoft.Extensions.DependencyInjection;

namespace ToyStream.Server.InMemory;

public static class DependencyInjectionExtensions
{

    public static IServiceCollection AddInMemoryServiceProviders(this IServiceCollection services)
    {
        services.AddSingleton<ITopicService, InMemoryTopicService>();
        return services;
    }
}