using ToyStream.Server;
using ToyStream.Server.Host;
using ToyStream.Server.InMemory;

var builder = Host.CreateApplicationBuilder(args);
builder.Services.AddSingleton<ConnectionAcceptor>();
builder.Services.AddSingleton<SocketListener>();
builder.Services.AddInMemoryServiceProviders();
builder.Services.AddHostedService<Worker>();

var host = builder.Build();
host.Run();