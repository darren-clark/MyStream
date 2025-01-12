// namespace ToyStream.Client;
//
// using System.IO.Pipelines;
// using Protocol;
//
// public class AdminClient: ClientBase
// {
//    
//     public AdminClient(PipeWriter writer): base(writer)
//     {
//     }
//
//     public async ValueTask CreateTopic(string topicName, short partitionCount,
//         CancellationToken cancellationToken = default)
//     {
//         ArgumentException.ThrowIfNullOrWhiteSpace(topicName);
//         ArgumentOutOfRangeException.ThrowIfGreaterThan(topicName.Length, 64);
//
//
//         var message = new CreateTopicMessage(topicName, partitionCount);
//         await SendMessage(message);
//     }
// }
//     
