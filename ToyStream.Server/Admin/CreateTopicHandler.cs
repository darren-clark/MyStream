using ToyStream.Protocol;
using ToyStream.Protocol.Messages;
using ToyStream.Protocol.Responses;
using ToyStream.Shared;

namespace ToyStream.Server;

public readonly struct CreateTopicHandler(ITopicService topicService) :
    IRequestHandler<CreateTopicMessage, AcknowledgeMessage<CreateTopicResult>>
{
    /// <summary>
    /// Bounded here rather than in the service: it is a protocol limit guarding the stack
    /// allocation in describe, so no provider should have to reimplement it.
    /// </summary>
    public ValueTask DispatchRequest(CreateTopicMessage message,
        ResponseWriter<AcknowledgeMessage<CreateTopicResult>> responseWriter,
        CancellationToken cancellationToken)
    {
        if (message.PartitionCount <= 0 || message.PartitionCount > ProtocolConstants.MaxPartitions)
        {
            responseWriter.WriteResponse(AcknowledgeMessage.WithResult(CreateTopicResult.InvalidPartitionCount));
            return default;
        }

        var created = topicService.TryCreateTopic(message.TopicName, message.PartitionCount);
        if (created.IsCompletedSuccessfully)
        {
            responseWriter.WriteResponse(AcknowledgeMessage.WithResult(ToResult(created.Result)));
            return default;
        }

        return AwaitAndAcknowledge(created, responseWriter);

        static CreateTopicResult ToResult(bool created) =>
            created ? CreateTopicResult.Success : CreateTopicResult.DuplicateTopic;

        static async ValueTask AwaitAndAcknowledge(ValueTask<bool> created,
            ResponseWriter<AcknowledgeMessage<CreateTopicResult>> responseWriter) =>
            responseWriter.WriteResponse(
                AcknowledgeMessage.WithResult(ToResult(await created.ConfigureAwait(false))));
    }
}
