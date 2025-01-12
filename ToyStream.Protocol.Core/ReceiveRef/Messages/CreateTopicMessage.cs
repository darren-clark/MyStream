namespace ToyStream.Protocol.ReceiveRef;

using System.Buffers;

public readonly ref struct CreateTopicMessage: IMessage<CreateTopicMessage>
{
    public Utf8String TopicName { get; private init; }
    public ShortValue PartitionCount { get; private init; }
    

    public static bool TryRead(SequenceReader<byte> reader, out CreateTopicMessage component, out int size)
    {
        if (!Utf8String.TryRead(ref reader, out var topic, out size))
        {
            component = default;
            return false;
        }

        if (!ShortValue.TryRead(ref reader, out var partitionCount, out size))
        {
            component = default;
            return false;
        }

        component = new CreateTopicMessage
        {
            TopicName = topic.Value,
            PartitionCount = partitionCount.Value,
        };
        
        // This is not a ref input, so return the amount read.
        size = (int) reader.Consumed;
        return true;
    }
}