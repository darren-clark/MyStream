using System.Diagnostics;

namespace MyStream.Protocol.Messages;

public readonly ref struct InitProducerMessage: IRequestMessage<InitProducerMessage>
{
    private readonly RefList<RefUtf8String> topics;

    private InitProducerMessage(short count, RefList<RefUtf8String> topics)
    {
        this.topics = topics;
    }
    
    static bool IMessage<InitProducerMessage>.TryRead<TReader>(ref TReader reader, out InitProducerMessage message, out int bytesToRead)
    {
        if (!reader.TryReadInt16(out var count, out bytesToRead))
        {
            message = default;
            return false;
        }
        
        var tryReader = reader;
        for (var i = 0; i < count; i++)
        {
            if (!tryReader.TryReadUtf8String(out var topic, out bytesToRead))
            {
                message = default;
                return false;
            }
        }

        var stringSize = tryReader.Consumed - reader.Consumed;        
        
        var read = reader.TryReadExact((int)stringSize, out var topics, out bytesToRead);
        Debug.Assert(read);

        message =  new InitProducerMessage (count, topics);
        return true;
    }

    public void Write<TWriter>(TWriter writer) where TWriter : struct, IPrimitiveWriter, allows ref struct
    {
        throw new NotImplementedException();
    }

    public static MessageType Type => MessageType.InitProducer;
}

public readonly ref struct InitProducerResponseMessage: IResponseMessage<InitProducerResponseMessage>
{
    private readonly Span<OwnedPartition> ownedPartitions;
    
    public static bool TryRead<TReader>(ref TReader reader, out InitProducerResponseMessage message, out int bytesToRead) where TReader : IPrimitiveReader, allows ref struct
    {
        if (!reader.TryReadInt16(out var count, out bytesToRead))
        {
            message = default;
            return false;
        }
        
        var tryReader = reader;
        if (!reader.TryRead)
        
        
    }

    public void Write<TWriter>(TWriter writer) where TWriter : struct, IPrimitiveWriter, allows ref struct
    {
        throw new NotImplementedException();
    }

    public static ResponseType Type => ResponseType.InitProducer;
}

public record struct OwnedPartition(int PartitionIndex, short Partition);