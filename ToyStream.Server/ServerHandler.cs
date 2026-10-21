using System.Buffers;
using System.IO.Pipelines;
using System.Net;
using ToyStream.Protocol;
using ToyStream.Protocol.Messages;
using ToyStream.Shared;
using ToyStream.Protocol.Responses;

namespace ToyStream.Server;

/// <summary>
/// Server role: its switch is exactly the request set, and implementing IRequestHandler for each arm
/// is what makes a missing handler a compile error.
/// </summary>
public readonly struct ServerHandler(ITopicService topicService) :
    IConnectionHandler,
    IRequestHandler<CreateTopicMessage, AcknowledgeMessage<CreateTopicResult>>,
    IRequestHandler<DescribeTopicRequestMessage, DescribeTopicResponseMessage>,
    IStreamingRequestHandler<PublishMessage, AcknowledgeMessage<PublishResult>>
{
    /// <summary>Roughly a kilobyte of descriptions; beyond this a describe rents instead.</summary>
    private const int MaxStackPartitions = 64;
    public DispatchResult TryParseAndDispatch(MessageType messageType, ref Dispatcher dispatch) =>
        messageType switch
        {
            MessageType.CreateTopic =>
                dispatch.Request<ServerHandler, CreateTopicMessage, AcknowledgeMessage<CreateTopicResult>>(this),
            MessageType.DescribeTopic =>
                dispatch.Request<ServerHandler, DescribeTopicRequestMessage, DescribeTopicResponseMessage>(this),
            MessageType.PublishEvent =>
                dispatch.StreamingRequest<ServerHandler, PublishMessage, AcknowledgeMessage<PublishResult>>(this),
            _ => dispatch.Unexpected(messageType)
        };

    /// <summary>
    /// Entirely synchronous: the descriptions live in stack memory the response message views, which
    /// cannot survive an await.
    /// </summary>
    ValueTask IRequestHandler<DescribeTopicRequestMessage, DescribeTopicResponseMessage>.DispatchRequest(
        DescribeTopicRequestMessage message,
        ResponseWriter<DescribeTopicResponseMessage> responseWriter,
        CancellationToken cancellationToken)
    {
        if (!topicService.TryGetTopic(message.TopicName, out var topic))
        {
            responseWriter.WriteResponse(new DescribeTopicResponseMessage
            {
                Result = DescribeTopicResult.UnknownTopic,
                Partitions = default
            });
            return default;
        }

        PartitionDescription[]? rented = null;
        var descriptions = topic.PartitionCount <= MaxStackPartitions
            ? stackalloc PartitionDescription[MaxStackPartitions]
            : (rented = ArrayPool<PartitionDescription>.Shared.Rent(topic.PartitionCount));

        try
        {
            descriptions = descriptions[..topic.PartitionCount];
            topic.Describe(descriptions);

            responseWriter.WriteResponse(new DescribeTopicResponseMessage
            {
                Result = DescribeTopicResult.Success,
                Partitions = new PartitionDescriptions(descriptions)
            });
            return default;
        }
        finally
        {
            if (rented is not null)
            {
                // The response is already copied into the pipe, so the buffer can go back now.
                ArrayPool<PartitionDescription>.Shared.Return(rented);
            }
        }
    }

    /// <summary>
    /// Bounded here rather than in the service: it is a protocol limit guarding the stack
    /// allocation in describe, so no provider should have to reimplement it.
    /// </summary>
    ValueTask IRequestHandler<CreateTopicMessage, AcknowledgeMessage<CreateTopicResult>>.DispatchRequest(
        CreateTopicMessage message,
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

    ValueTask IStreamingRequestHandler<PublishMessage, AcknowledgeMessage<PublishResult>>.DispatchRequest(
        StreamingMessage<PublishMessage> message,
        ResponseWriter<AcknowledgeMessage<PublishResult>> responseWriter,
        CancellationToken cancellationToken)
    {
        var header = message.Header;

        // Composed from lookups rather than one service call, so the service never needs to know
        // which protocol result a missing topic or partition maps to.
        if (!topicService.TryGetTopic(header.Topic, out var topic))
        {
            responseWriter.WriteResponse(AcknowledgeMessage.WithResult(PublishResult.UnknownTopic));
            return default;
        }

        if (!topic.TryGetPartition(header.Partition, out var partition))
        {
            responseWriter.WriteResponse(AcknowledgeMessage.WithResult(PublishResult.UnknownPartition));
            return default;
        }

        return WriteAndAcknowledge(message.Body, partition.GetEventWriter(header.Key, message.Size),
            responseWriter, cancellationToken);

        static async ValueTask WriteAndAcknowledge(PipeReader body, IEventWriter writer,
            ResponseWriter<AcknowledgeMessage<PublishResult>> responseWriter, CancellationToken cancellationToken)
        {
            await writer.WriteEvent(body, cancellationToken).ConfigureAwait(false);
            responseWriter.WriteResponse(AcknowledgeMessage.WithResult(PublishResult.Success));
        }
    }
}