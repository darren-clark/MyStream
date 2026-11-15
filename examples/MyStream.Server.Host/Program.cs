using MyStream.Server;

var builder = Host.CreateApplicationBuilder(args);
builder.Services.AddMyStreamServer()
    .WithTcpListener();

var host = builder.Build();
host.Run();
