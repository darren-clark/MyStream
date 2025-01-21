namespace ToyStream.Protocol;

using Messages;

public interface IToyProtocolHandler
{
    public ValueTask HandleCreateTopicMessage(ref readonly CreateTopicMessage message);
    public ValueTask HandleResponseMessage<T>(ref readonly T message) where T : struct, IResponseMessage<T>;
}