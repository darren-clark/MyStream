using MyStream.Protocol;
using MyStream.Protocol.Messages;
using MyStream.Protocol.Primitives;
using MyStream.Server.Topics;

namespace MyStream.Server.Roles.Producer;

internal struct InitHandler(ProducerConnection connection): IRequestHandler<InitProducerMessage, InitProducerResponseMessage>
{
    public ValueTask DispatchRequest(InitProducerMessage message, ResponseWriter<InitProducerResponseMessage> responseWriter,
        CancellationToken cancellationToken)
    {
        var topics = new List<Utf8String>();
        
        throw new NotImplementedException();
    }
}