using MyStream.Protocol.Messages;

namespace MyStream.Protocol.Responses;

/// <summary>
/// Basic ACK.
/// </summary>
public readonly struct AcknowledgeResponse : IResponse<AcknowledgeMessage, AcknowledgeResponse>
{
    static AcknowledgeResponse IResponse<AcknowledgeMessage, AcknowledgeResponse>.CreateResponse(
        ref readonly AcknowledgeMessage response) => new();
}

/// <summary>
/// Generic acknowledge, with a result code.
/// </summary>
/// <param name="result"></param>
/// <typeparam name="TResult"></typeparam>
public readonly struct AcknowledgeResponse<TResult>(TResult result)
    : IResponse<AcknowledgeMessage<TResult>, AcknowledgeResponse<TResult>>
    where TResult : unmanaged, Enum
{
    public TResult Result { get; } = result;

    public static AcknowledgeResponse<TResult> CreateResponse(ref readonly AcknowledgeMessage<TResult> response)
        => new AcknowledgeResponse<TResult>(response.Result);
}