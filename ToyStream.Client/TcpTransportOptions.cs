namespace ToyStream.Client;

public class TcpTransportOptions
{
    public required string Host { get; init; }
    public required int Port { get; init; }
}