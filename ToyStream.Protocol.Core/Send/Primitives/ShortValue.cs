namespace ToyStream.Protocol.Send;

using Shared;

public  readonly ref struct ShortValue: IProtocolComponent
{
    private readonly short value;

    public ShortValue(short value)
    {
        this.value = value;
    }

    public int Size => sizeof(short);
    
    public void Write(ref SpanWriter writer)
    {
        writer.Write(this.value);
    }
}