using Microsoft.Extensions.DependencyInjection;
using ToyStream.Server;

namespace ToyStream;

public static class DependencyInjectionExtensions
{
    public static IServiceCollection AddToyStreamServer(this IServiceCollection serviceCollection)
    {
        serviceCollection.AddSingleton<ConnectionAcceptor>();
        serviceCollection.AddSingleton<ConsumerGroupCoordinator>();
        return serviceCollection;
    }
}