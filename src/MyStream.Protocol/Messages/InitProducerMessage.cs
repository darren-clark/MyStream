using MyStream.Protocol.Primitives;

namespace MyStream.Protocol.Messages;

public readonly ref struct InitProducerMessage: IRequestMessage<InitProducerMessage>
{
    private readonly PrimitiveList<Utf8String,RefUtf8String> topics;

    private InitProducerMessage(PrimitiveList<Utf8String, RefUtf8String> topics)
    {
        this.topics = topics;
    }
    
    static bool IProtocolPrimitive<InitProducerMessage>.TryRead<TReader>(scoped ref TReader reader, out InitProducerMessage message, out int bytesToRead)
    {
        if (!reader.TryReadPrimitive<PrimitiveList<Utf8String, RefUtf8String>>(out var topics, out bytesToRead))
        {
            message = default;
            return false;
        }
        message =  new InitProducerMessage (topics);
        return true;
    }

    public void Write<TWriter>(TWriter writer) where TWriter : struct, IPrimitiveWriter, allows ref struct
    {
        writer.Write(topics);
    }

    public static MessageType Type => MessageType.InitProducer;
}

 public readonly ref struct InitProducerResponseMessage: IResponseMessage<InitProducerResponseMessage>
 {
     public required PrimitiveList<OwnedPartition>  OwnedPartitions { get; init; }

     public static bool TryRead<TReader>(scoped ref TReader reader, out InitProducerResponseMessage value, out int bytesToRead) where TReader : IPrimitiveReader, allows ref struct
     {
         if (reader.TryReadPrimitive(out PrimitiveList<OwnedPartition> ownedPartitions, out bytesToRead))
         {
             value = new InitProducerResponseMessage { OwnedPartitions = ownedPartitions };
             return true;
         }
         
         value = default;
         return false;
     }
     
     public void Write<TWriter>(TWriter writer) where TWriter : struct, IPrimitiveWriter, allows ref struct
      => writer.Write(OwnedPartitions);

     public static ResponseType Type => ResponseType.InitProducer;



 }

     public readonly record struct OwnedPartition(short PartitionIndex, short Partition) : IProtocolPrimitive<OwnedPartition>
     {
         public void Write<TWriter>(TWriter writer) where TWriter : struct, IPrimitiveWriter, allows ref struct
         {
             writer.Write(PartitionIndex);
             writer.Write(Partition);
         }

         public static bool TryRead<TReader>(scoped ref TReader reader, out OwnedPartition value, out int bytesToRead) where TReader : IPrimitiveReader, allows ref struct
         {
             if (reader.TryReadInt16(out var partitionIndex, out bytesToRead) &&
                 reader.TryReadInt16(out var partition, out bytesToRead))
             {
                 value = new OwnedPartition(partitionIndex, partition);
                 return true;
             }

             value = default;
             return false;
         }
     }