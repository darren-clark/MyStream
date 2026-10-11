namespace ToyStream.Protocol;

public interface IRequestMessage<TSelf, TResponseMessage> : IMessage<TSelf>
    where TSelf : struct, IRequestMessage<TSelf, TResponseMessage>, allows ref struct
    where TResponseMessage : struct, IResponseMessage<TResponseMessage>, allows ref struct
{
    internal static abstract MessageType Type { get; }
}
