namespace ToyStream.Protocol;

public interface IResponseMessage<TSelf> : IMessage<TSelf>
    where TSelf : struct, IResponseMessage<TSelf>, allows ref struct
{
    internal static abstract ResponseType Type { get; }
}