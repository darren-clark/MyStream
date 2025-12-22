// namespace ToyStream.Protocol.Messages;
//
// using Responses;
//
// public ref struct PublishMessage<TReader>: IRequestMessage<PublishMessage<TReader>, AcknowledgeMessage, AcknowledgeResponse>
// {
//     private long size;
//     
//     static bool IMessage<PublishMessage>.TryRead<TReader>(ref TReader reader, out PublishMessage message, out int bytesToRead)
//     {
//         if (!reader.TryReadInt64(out var size, out bytesToRead))
//         {
//             message = default;
//             bytesToRead = default;
//             return false;
//         }
//
//         message = new PublishMessage()
//         {
//             size = size,
//             reader = reader
//         };
//     }
//
//     public MessageType Type => MessageType.Publish;
//     
//     void IMessage.Write<TWriter>(TWriter writer)
//     {
//         throw new NotImplementedException();
//     }
// }