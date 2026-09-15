namespace ToyStream.Protocol.Benchmark.Scale;

/// <summary>
/// One interface per message, so a connection implements only the messages it actually serves.
/// Handle is called synchronously while the message is still a valid view over the pipe buffer;
/// anything that must outlive that has to be copied out before the returned task completes.
/// </summary>
public interface IHandler<TMessage>
    where TMessage : struct, IMessage<TMessage>, allows ref struct
{
    ValueTask Handle(ref readonly TMessage message);
}
