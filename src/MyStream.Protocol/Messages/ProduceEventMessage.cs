using MyStream.Protocol.Primitives;

namespace MyStream.Protocol.Messages;

public enum ProduceResult
{
    Success,
    InvalidTopic,
    InvalidPartition
}

public ref struct ProduceEventMessage: IStreamingRequestMessage<ProduceEventMessage, AcknowledgeMessage<ProduceResult>>
{
    public required short TopicIndex { get; init; }
    public required short Partition { get; init; }
    
    public required RefBytes Key { get; init; }

    public static MessageType Type => MessageType.ProduceEvent;

    public static bool TryRead<TReader>(scoped ref TReader reader, out ProduceEventMessage value, out int bytesToRead) where TReader : IPrimitiveReader, allows ref struct
    {
        if (reader.TryReadInt16(out short topicIndex, out bytesToRead) &&
            reader.TryReadInt16(out short partition, out bytesToRead) &&
            reader.TryReadPrimitive(out RefBytes key, out bytesToRead))
        {
            value = new ProduceEventMessage { TopicIndex = topicIndex, Partition = partition, Key = key };
            return true;
        }
        
        value = default;
        return false;
    }

    public void Write<TWriter>(TWriter writer) where TWriter : struct, IPrimitiveWriter, allows ref struct
    {
        writer.Write(TopicIndex);
        writer.Write(Partition);
        writer.Write(Key);
    }
}

