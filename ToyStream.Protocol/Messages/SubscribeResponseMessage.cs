using ToyStream.Protocol.Responses;

namespace ToyStream.Protocol.Messages;

public struct SubscribeResponseMessage : IResponseMessage<SubscribeResponseMessage>
{
    static bool IMessage<SubscribeResponseMessage>.TryRead<TReader>(ref TReader reader, out SubscribeResponseMessage message, out int bytesToRead)
    {
        throw new NotImplementedException();
    }

    public void Write<TWriter>(TWriter writer) where TWriter : struct, IPrimitiveWriter, allows ref struct
    {
        throw new NotImplementedException();
    }

    public static ResponseType Type { get; }
}