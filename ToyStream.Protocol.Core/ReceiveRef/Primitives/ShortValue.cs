namespace ToyStream.Protocol.ReceiveRef;

using System.Buffers;

public readonly ref struct ShortValue : IProtocolComponent<ShortValue>
{
    private readonly short value;

    public ShortValue(short value)
    {
        this.value = value;
    }
    
    public short Value => this.value;
    
    public static bool TryRead(scoped ref SequenceReader<byte> reader, out ShortValue component, out int bytesToRead)
    {
        if (reader.Remaining < 2)
        {
            bytesToRead = (int)(2 - reader.Remaining);
            component = default;
            return false;
        }

        bytesToRead = 2;
        component = new ShortValue(reader.UnsafeReadInt16BigEndian());
        return true;
    }
}