using MyStream.Protocol;
using MyStream.Protocol.Messages;
using MyStream.Protocol.Responses;
using MyStream.Server.Topics;

namespace MyStream.Server.Roles.Admin;

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
        if (message.PartitionCount is <= 0 or > ProtocolConstants.MaxPartitions)
        {
            responseWriter.WriteResponse(AcknowledgeMessage.WithResult(CreateTopicResult.InvalidPartitionCount));
            return default;
        }

        var created = topicService.TryCreateTopic(message.TopicName, message.PartitionCount);
        if (!created.IsCompletedSuccessfully)
        {
            return AwaitAndAcknowledge(created, responseWriter);
        }

        responseWriter.WriteResponse(AcknowledgeMessage.WithResult(ToResult(created.Result)));
        return default;

        static CreateTopicResult ToResult(bool created) =>
            created ? CreateTopicResult.Success : CreateTopicResult.DuplicateTopic;

        static async ValueTask AwaitAndAcknowledge(ValueTask<bool> created,
            ResponseWriter<AcknowledgeMessage<CreateTopicResult>> responseWriter) =>
            responseWriter.WriteResponse(
                AcknowledgeMessage.WithResult(ToResult(await created.ConfigureAwait(false))));
    }
}
