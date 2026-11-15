using System.IO.Pipelines;

using MyStream.Protocol;
using MyStream.Protocol.Messages;
using MyStream.Protocol.Responses;
using MyStream.Server.Topics;

namespace MyStream.Server.Roles.Admin;

public readonly struct AdminHandler(ITopicService topicService) : IConnectionHandler
{
    private readonly CreateTopicHandler createTopic = new(topicService);
    private readonly DescribeTopicHandler describeTopic = new(topicService);


    public DispatchResult TryParseAndDispatch(MessageType messageType, ref Dispatcher dispatch)
        =>
            messageType switch
            {
                MessageType.CreateTopic => dispatch
                    .Request<CreateTopicHandler, CreateTopicMessage, AcknowledgeMessage<CreateTopicResult>>(
                        this.createTopic),
                MessageType.DescribeTopic =>
                    dispatch.Request<DescribeTopicHandler, DescribeTopicRequestMessage, DescribeTopicResponseMessage>(
                        this.describeTopic),
                _ => dispatch.Unexpected(messageType)
            };


    // No initialization expected here.
    public Task<HandshakeStatus> Initialize(PipeReader reader, CancellationToken cancellationToken)
    {
        return Task.FromResult(HandshakeStatus.Accepted);
    }
}