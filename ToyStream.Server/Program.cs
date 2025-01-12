using ToyStream.Server;

var builder = Host.CreateApplicationBuilder(args);
builder.Services.AddSingleton<IServerCore, ServerCore>();
builder.Services.AddSingleton<ClientHandlerFactory>();
    
builder.Services.AddHostedService<Worker>();

var host = builder.Build();
host.Run();