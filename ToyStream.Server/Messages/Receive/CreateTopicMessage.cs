namespace ToyStream.Protocol.Receive;

using System.Buffers;
using System.Diagnostics.CodeAnalysis;

public readonly struct CreateTopicMessage: IMessage, IProtocolComponent<CreateTopicMessage>
{
    public Utf8StringValue TopicName { get; private init; }
    public ShortValue PartitionCount { get; private init; }


    public static bool TryRead(ref SequenceReader<byte> reader, [NotNullWhen(true)] out CreateTopicMessage? component, out int bytesToRead)
    {
        var tryReader = reader;
        if (!Utf8StringValue.TryRead(ref tryReader, out var topic, out bytesToRead))
        {
            component = default;
            return false;
        }

        if (!ShortValue.TryRead(ref tryReader, out var partitionCount, out bytesToRead))
        {
            component = default;
            return false;
        }

        reader = tryReader;
        component = new CreateTopicMessage
        {
            TopicName = topic.Value,
            PartitionCount = partitionCount.Value,
        };
        return true;
    }
    
    public void Dispose()
    {
        this.TopicName.Dispose();
    }
}