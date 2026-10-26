using System.IO.Pipelines;

namespace ToyStream.Protocol;

/// <summary>
/// A connected duplex byte stream together with whatever owns it, so neither half of the protocol
/// needs to know whether it is a socket, a loopback pipe or a test double.
/// </summary>
public interface ITransportConnection : IDuplexPipe, IAsyncDisposable;
