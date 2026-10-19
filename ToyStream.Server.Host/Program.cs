using ToyStream.Server.Host;

var builder = Host.CreateApplicationBuilder(args);
builder.Services.AddSingleton<ClientHandlerFactory>();
builder.Services.AddSingleton<SocketListener>();
    
builder.Services.AddHostedService<Worker>();

var host = builder.Build();
host.Run();