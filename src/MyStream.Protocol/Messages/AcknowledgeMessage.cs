using MyStream.Protocol.Primitives;

namespace MyStream.Protocol.Messages;

/// <summary>
/// Basic ACK.
/// </summary>
public readonly ref struct AcknowledgeMessage: IResponseMessage<AcknowledgeMessage>
{
    public static ResponseType Type => ResponseType.Acknowledge;
    
    static bool IProtocolPrimitive<AcknowledgeMessage>.TryRead<TReader>(scoped ref TReader reader, out AcknowledgeMessage message, out int bytesToRead)
    {
        message = new AcknowledgeMessage();
        bytesToRead = 0;
        return true;
    }

    void IProtocolPrimitive<AcknowledgeMessage>.Write<TWriter>(TWriter writer)
    {
    }
    
    public static AcknowledgeMessage<TResult> WithResult<TResult>(TResult result) where TResult : unmanaged, Enum =>
        new(result);
   
}

/// <summary>
/// Generic acknowledge with a single enum result.
/// </summary>
/// <param name="result"></param>
/// <typeparam name="TResult"></typeparam>
public readonly ref struct AcknowledgeMessage<TResult>(TResult result) : IResponseMessage<AcknowledgeMessage<TResult>>
    where TResult : unmanaged, Enum
{
    static bool IProtocolPrimitive<AcknowledgeMessage<TResult>>.TryRead<TReader>(scoped ref TReader reader, out AcknowledgeMessage<TResult> message, out int bytesToRead)
    {
        if (reader.TryReadEnum<TResult>(out var enumValue, out bytesToRead))
        {
            message = new AcknowledgeMessage<TResult>(enumValue);
            return true;
        }
        
        message = default;
        return false;
    }

    public TResult Result { get; init; } = result;

    public static ResponseType Type => ResponseType.AcknowledgeWithResult;

    public void Write<TWriter>(TWriter writer) where TWriter : struct, IPrimitiveWriter, allows ref struct
    {
        writer.WriteEnum(Result);
    }
}