using System.Net;

namespace MyStream.Server;

public class ServerTcpOptions
{
    public required int Port { get; init; }
    public required IPAddress? Address { get; init; } 
}