namespace MyStream.Protocol.Messages;

/// <summary>
/// Basic ACK.
/// </summary>
public readonly ref struct AcknowledgeMessage: IResponseMessage<AcknowledgeMessage>
{
    public static ResponseType Type => ResponseType.Acknowledge;
    
    static bool IMessage<AcknowledgeMessage>.TryRead<TReader>(ref TReader reader, out AcknowledgeMessage message, out int bytesToRead)
    {
        message = new AcknowledgeMessage();
        bytesToRead = 0;
        return true;
    }

    void IMessage.Write<TWriter>(TWriter writer)
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
    static bool IMessage<AcknowledgeMessage<TResult>>.TryRead<TReader>(ref TReader reader, out AcknowledgeMessage<TResult> message, out int bytesToRead)
    {
        if (!reader.TryReadEnum<TResult>(out var enumValue, out bytesToRead))
        {
            message = default;
            return false;
        }
        
        message = new AcknowledgeMessage<TResult>(enumValue);
        return true;
    }

    public TResult Result { get; init; } = result;

    public static ResponseType Type => ResponseType.AcknowledgeWithResult;

    public void Write<TWriter>(TWriter writer) where TWriter : struct, IPrimitiveWriter, allows ref struct
    {
        writer.Write(Result);
    }
}