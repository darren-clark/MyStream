namespace ToyStream.Protocol.Benchmark;

using System.IO.Pipelines;
using ToyStream.Protocol;
using ToyStream.Protocol.Messages;
using ToyStream.Protocol.Responses;

/// <summary>Shared by the benchmark and the JIT dump driver so both measure the same code.</summary>
internal readonly struct BenchmarkServerHandler
    : IConnectionHandler, IRequestHandler<CreateTopicMessage, AcknowledgeMessage>
{
    public DispatchResult TryParseAndDispatch(MessageType messageType, ref PrimitiveReader reader,
        PrimitiveWriter writer, CancellationToken cancellationToken) =>
        messageType switch
        {
            MessageType.CreateTopic => ConnectionHandler
                .TryParseAndDispatchRequest<BenchmarkServerHandler, CreateTopicMessage, AcknowledgeMessage>(
                    this, writer, ref reader, cancellationToken),
            _ => ConnectionHandler.ThrowUnexpectedMessage(messageType)
        };

    public ValueTask DispatchRequest(CreateTopicMessage message,
        ResponseWriter<AcknowledgeMessage> responseWriter, CancellationToken cancellationToken)
    {
        responseWriter.WriteResponse(new AcknowledgeMessage());
        return default;
    }
}

internal readonly struct BenchmarkClientHandler(PendingRequests pending) : IConnectionHandler
{
    public DispatchResult TryParseAndDispatch(MessageType messageType, ref PrimitiveReader reader,
        PrimitiveWriter writer, CancellationToken cancellationToken) =>
        messageType switch
        {
            MessageType.Acknowledge => ConnectionHandler
                .TryParseAndCompleteResponse<AcknowledgeMessage, AcknowledgeResponse>(pending, ref reader),
            _ => ConnectionHandler.ThrowUnexpectedMessage(messageType)
        };
}
