namespace ToyStream.Client;

using System.IO.Pipelines;
using System.Net.Sockets;

public sealed class TcpClientTransport(ClientOptions options) : IClientTransport
{
    public async ValueTask<ITransportConnection> ConnectAsync(CancellationToken cancellationToken)
    {
        // NoDelay: requests are small and latency-bound, so Nagle would sit on them waiting for more.
        var socket = new Socket(SocketType.Stream, ProtocolType.Tcp) { NoDelay = true };
        try
        {
            await socket.ConnectAsync(options.Host, options.Port, cancellationToken).ConfigureAwait(false);
        }
        catch
        {
            socket.Dispose();
            throw;
        }

        return new TcpTransportConnection(socket);
    }

    private sealed class TcpTransportConnection : ITransportConnection
    {
        private readonly NetworkStream stream;

        public TcpTransportConnection(Socket socket)
        {
            this.stream = new NetworkStream(socket, ownsSocket: true);
            Input = PipeReader.Create(this.stream);
            Output = PipeWriter.Create(this.stream);
        }

        public PipeReader Input { get; }

        public PipeWriter Output { get; }

        public async ValueTask DisposeAsync()
        {
            await Input.CompleteAsync().ConfigureAwait(false);
            await Output.CompleteAsync().ConfigureAwait(false);
            await this.stream.DisposeAsync().ConfigureAwait(false);
        }
    }
}
