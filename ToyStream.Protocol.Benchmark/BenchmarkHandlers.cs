namespace ToyStream.Protocol.Benchmark;

using System.IO.Pipelines;
using ToyStream.Protocol;
using ToyStream.Protocol.Messages;
using ToyStream.Protocol.Responses;

/// <summary>Shared by the benchmark and the JIT dump driver so both measure the same code.</summary>
internal readonly struct BenchmarkServerHandler
    : IConnectionHandler, IRequestHandler<CreateTopicMessage, AcknowledgeMessage<CreateTopicResult>>
{
    public DispatchResult TryParseAndDispatch(MessageType messageType, ref PrimitiveReader reader,
        PrimitiveWriter writer, MessageBodyReader body, CancellationToken cancellationToken) =>
        messageType switch
        {
            MessageType.CreateTopic => ConnectionHandler
                .TryParseAndDispatchRequest<BenchmarkServerHandler, CreateTopicMessage, AcknowledgeMessage<CreateTopicResult>>(
                    this, writer, ref reader, cancellationToken),
            _ => ConnectionHandler.ThrowUnexpectedMessage(messageType)
        };

    // Goes through AwaitResultAndWriteAcknowledge rather than writing directly, so the benchmark
    // exercises the same path as ServerHandler.
    public ValueTask DispatchRequest(CreateTopicMessage message,
        ResponseWriter<AcknowledgeMessage<CreateTopicResult>> responseWriter, CancellationToken cancellationToken)
        => ConnectionHandler.AwaitResultAndWriteAcknowledge(
            new ValueTask<CreateTopicResult>(CreateTopicResult.Success), responseWriter);
}

internal readonly struct BenchmarkClientHandler : IConnectionHandler
{
    public DispatchResult TryParseAndDispatch(MessageType messageType, ref PrimitiveReader reader,
        PrimitiveWriter writer, MessageBodyReader body, CancellationToken cancellationToken) =>
        ConnectionHandler.ThrowUnexpectedMessage(messageType);
}
