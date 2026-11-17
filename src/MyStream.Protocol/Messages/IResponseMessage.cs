using MyStream.Protocol.Primitives;

namespace MyStream.Protocol.Messages;

/// <summary>
/// Response to a request.
/// </summary>
public interface IResponseMessage<TSelf> : IProtocolPrimitive<TSelf>
    where TSelf : struct, IResponseMessage<TSelf>, allows ref struct
{
    internal static abstract ResponseType Type { get; }
}