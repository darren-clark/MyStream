namespace ToyStream.Protocol.Responses;

using Messages;

// public readonly ref struct AcknowledgeMessage: IMessage<AcknowledgeMessage>
// {
//     public MessageType Type => MessageType.Acknowledge;
//     static bool IMessage<AcknowledgeMessage>.TryRead<TReader>(ref TReader reader, out AcknowledgeMessage message, out int bytesToRead)
//     {
//         message = new AcknowledgeMessage();
//         bytesToRead = 0;
//         return true;
//     }
//
//     internal void Write<TWriter>(TWriter writer) where TWriter: IPrimitiveWriter, allows ref struct
//     {
//     }
//
//     void IMessage<AcknowledgeMessage>.Write<TWriter>(TWriter writer)
//     {
//         Write(writer);
//     }
//     
//     public void Dispose() { }
// }

public class AcknowledgeResponse
{
    internal static readonly AcknowledgeResponse Instance = new AcknowledgeResponse();
}