using System.Collections.Concurrent;

namespace ToyStream.Protocol.Responses;

using Messages;

public struct AcknowledgeResponse : IResponse<AcknowledgeMessage, AcknowledgeResponse>
{
    internal static readonly AcknowledgeResponse Instance = new AcknowledgeResponse();

    static AcknowledgeResponse IResponse<AcknowledgeMessage, AcknowledgeResponse>.CreateResponse(
        ref readonly AcknowledgeMessage response) => Instance;
}

public class AcknowledgeResponse<TResult>(TResult result)
    : IResponse<AcknowledgeMessage<TResult>, AcknowledgeResponse<TResult>>
    where TResult : unmanaged, Enum
{
    public TResult Result { get; } = result;

    public static AcknowledgeResponse<TResult> CreateResponse(ref readonly AcknowledgeMessage<TResult> response)
        => new AcknowledgeResponse<TResult>(response.Result);
}