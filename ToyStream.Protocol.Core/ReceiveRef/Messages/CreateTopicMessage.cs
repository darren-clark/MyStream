namespace ToyStream.Protocol.ReceiveRef;

using System.Buffers;

public readonly ref struct CreateTopicMessage: IMessage<CreateTopicMessage>
{
    public Utf8StringValue TopicName { get; private init; }
    public ShortValue PartitionCount { get; private init; }
    

    
    public static bool TryRead(scoped ref SequenceReader<byte> reader, out CreateTopicMessage component, out int bytesToRead)
    {
        if (!Utf8StringValue.TryRead(ref reader, out var topic, out bytesToRead))
        {
            component = default;
            return false;
        }

        if (!ShortValue.TryRead(ref reader, out var partitionCount, out bytesToRead))
        {
            component = default;
            return false;
        }

        component = new CreateTopicMessage
        {
            TopicName = topic,
            PartitionCount = partitionCount,
        };

        bytesToRead = 0;
        return true;
    }
}