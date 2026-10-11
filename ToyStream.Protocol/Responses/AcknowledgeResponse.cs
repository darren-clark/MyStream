using System.Collections.Concurrent;

namespace ToyStream.Protocol.Responses;

using Messages;

public class AcknowledgeResponse : IResponse<AcknowledgeMessage, AcknowledgeResponse>
{
    internal static readonly AcknowledgeResponse Instance = new AcknowledgeResponse();

    static AcknowledgeResponse IResponse<AcknowledgeMessage, AcknowledgeResponse>.CreateResponse(
        ref readonly AcknowledgeMessage response) => Instance;
}

public class AcknowledgeResponse<TResult>: IResponse<AcknowledgeMessage<TResult>, AcknowledgeResponse<TResult>> where TResult : unmanaged, Enum
{
    private static ConcurrentDictionary<TResult, AcknowledgeResponse<TResult>> cache = new ConcurrentDictionary<TResult, AcknowledgeResponse<TResult>>();
    
    public AcknowledgeResponse(TResult result)
    {
        Result = result;
    }

    public TResult Result { get; }
    
    public static AcknowledgeResponse<TResult> CreateResponse(ref readonly AcknowledgeMessage<TResult> response)
    {
        return cache.GetOrAdd(response.Result, result => new AcknowledgeResponse<TResult>(result));
    }
}