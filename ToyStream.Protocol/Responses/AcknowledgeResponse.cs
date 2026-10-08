namespace ToyStream.Protocol.Responses;

using Messages;

public class AcknowledgeResponse : IResponse<AcknowledgeMessage, AcknowledgeResponse>
{
    internal static readonly AcknowledgeResponse Instance = new AcknowledgeResponse();

    static AcknowledgeResponse IResponse<AcknowledgeMessage, AcknowledgeResponse>.CreateResponse(
        ref readonly AcknowledgeMessage response) => Instance;
}