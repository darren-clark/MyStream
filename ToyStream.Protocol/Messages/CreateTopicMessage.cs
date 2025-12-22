namespace ToyStream.Protocol.Messages;

using Shared;

public readonly ref struct CreateTopicMessage : IRequestMessage<CreateTopicMessage, CreateTopicResponseMessage>
{
    public required RefUtf8String TopicName { get; init; }
    public required short PartitionCount { get; init; }

    public MessageType Type => MessageType.CreateTopic;

    static bool IMessage<CreateTopicMessage>.TryRead<TReader>(ref TReader reader, out CreateTopicMessage message,
        out int bytesToRead)
    {
        var tryReader = reader;
        if (!tryReader.TryReadUtf8String(out var topicName, out bytesToRead))
        {
            message = default;
            return false;
        }

        if (!tryReader.TryReadInt16(out var partitionCount, out bytesToRead))
        {
            message = default;
            return false;
        }

        reader = tryReader;
        message = new CreateTopicMessage
        {
            TopicName = topicName,
            PartitionCount = partitionCount
        };
        return true;
    }

    internal void Write<TWriter>(TWriter writer) where TWriter : IPrimitiveWriter, allows ref struct
    {
        writer.Write(TopicName);
        writer.Write(PartitionCount);
    }

    void IMessage.Write<TWriter>(TWriter writer) => Write(writer);
}

public enum CreateTopicResult : byte
{
    TopicCreated,
    DuplicateTopic,
    Error
}

public ref struct CreateTopicResponseMessage : IResponseMessage<CreateTopicResponseMessage>
{
    public required CreateTopicResult Result { get; init; }

    static bool IMessage<CreateTopicResponseMessage>.TryRead<TReader>(ref TReader reader, out CreateTopicResponseMessage message, out int bytesToRead)
    {
        if (!reader.TryReadEnum<CreateTopicResult>(out var result, out bytesToRead))
        {
            message = default;
            return false;
        }

        message = new CreateTopicResponseMessage
        {
            Result = result
        };
        return true;
    }

    public MessageType Type => MessageType.CreateTopic;

    internal readonly void Write<TWriter>(TWriter writer) where TWriter : struct, IPrimitiveWriter, allows ref struct
    {
        writer.Write(Result);        
    }

    void IMessage.Write<TWriter>(TWriter writer) => Write(writer);
}

public class CreateTopicResponse : IResponse<CreateTopicResponseMessage, CreateTopicResponse>
{
    public required CreateTopicResult Result { get; init; }
    
    public static CreateTopicResponse CreateResponse(ref readonly CreateTopicResponseMessage response)
        => new CreateTopicResponse
        {
            Result = response.Result
        };

}