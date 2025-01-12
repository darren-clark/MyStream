namespace ToyStream.Protocol.Receive;

using System.Buffers;
using System.Diagnostics.CodeAnalysis;

public readonly struct ShortValue : IProtocolComponent<ShortValue>
{
    private readonly short value;

    public ShortValue(short value)
    {
        this.value = value;
    }
    
    public static implicit operator ShortValue(short value) => new (value);
    public static implicit operator short(ShortValue value) => value.value;

    public short Value => this.value;
    
    public static bool TryRead(ref SequenceReader<byte> reader, [NotNullWhen(true)] out ShortValue? component, out int bytesToRead)
    {
        (component, bytesToRead) = reader.Remaining switch
        {
            0 => ((ShortValue?)null, 2),
            1 => (null, 1),
            _ => (reader.UnsafeReadInt16BigEndian(), 0)
        };
        
        return component is not null;
    }
}