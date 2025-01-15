namespace ToyStream.Protocol.Primitives;

using Shared;

public readonly struct ShortValue
{
    private readonly short value;

    public ShortValue(short value)
    {
        this.value = value;
    }

    public int Size => sizeof(short);

    public short Value => this.value;
    
    public void Write(ref BigEndianSpanWriter writer)
    {
        writer.Write(this.value);
    }
}