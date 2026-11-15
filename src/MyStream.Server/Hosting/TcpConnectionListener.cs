using System.IO.Pipelines;
using System.Net;
using System.Net.Sockets;

using Microsoft.Extensions.Hosting;
using Microsoft.Extensions.Logging;
using Microsoft.Extensions.Options;

using ToyStream.Protocol;

namespace MyStream.Server.Hosting;

public class TcpConnectionListener(ILogger<TcpConnectionListener> logger, IHostEnvironment environment, IOptions<ServerTcpOptions> options): IConnectionListener 
{
    public async Task Listen(Action<ITransportConnection> accept, CancellationToken cancellationToken)
    {
        var address = options.Value.Address ?? (environment.IsDevelopment() ? IPAddress.Loopback : IPAddress.Any);
        logger.LogInformation("MyStream listener starting on {Address}:{Port}", address, options.Value.Port);

        using var socket = new TcpListener(address, options.Value.Port);
        socket.Start();
        while (!cancellationToken.IsCancellationRequested)
        {
            var client = await socket.AcceptSocketAsync(cancellationToken);
            accept(new SocketTransport(client));
        }        
    }

    private class SocketTransport : ITransportConnection
    {
        private readonly NetworkStream networkStream;
        
        public SocketTransport(Socket socket)
        {
            networkStream = new NetworkStream(socket, true);
            Input = PipeReader.Create(networkStream);
            Output = PipeWriter.Create(networkStream);
        }
        
        public ValueTask DisposeAsync()
        {
            networkStream.Dispose();
            return default;
        }

        public PipeReader Input { get; }
        public PipeWriter Output { get; }
    }
}