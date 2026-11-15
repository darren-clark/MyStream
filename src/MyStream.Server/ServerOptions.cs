namespace MyStream.Server;

public class ServerOptions
{
    public TimeSpan HandshakeTimeout { get; set; } = TimeSpan.FromSeconds(10);
}