namespace ToyStream.Server.Host;

using System.IO.Pipelines;
using System.Net.Sockets;
using ToyStream.Protocol;

/// <summary>
/// The only socket-aware part of serving a connection: everything past the pipe pair is the same
/// whatever the peer connected over.
/// </summary>
internal sealed class SocketConnection : ITransportConnection
{
    private readonly NetworkStream stream;

    public SocketConnection(Socket socket)
    {
        this.stream = new NetworkStream(socket, ownsSocket: true);
        this.Input = PipeReader.Create(this.stream);
        this.Output = PipeWriter.Create(this.stream);
    }

    public PipeReader Input { get; }

    public PipeWriter Output { get; }

    public ValueTask DisposeAsync() => this.stream.DisposeAsync();
}
