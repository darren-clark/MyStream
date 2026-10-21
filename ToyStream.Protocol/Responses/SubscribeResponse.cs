using ToyStream.Protocol.Messages;

namespace ToyStream.Protocol.Responses;

public struct SubscribeResponse:IResponse<SubscribeResponseMessage, SubscribeResponse>
{
    public static SubscribeResponse CreateResponse(ref readonly SubscribeResponseMessage response)
    {
        throw new NotImplementedException();
    }
}