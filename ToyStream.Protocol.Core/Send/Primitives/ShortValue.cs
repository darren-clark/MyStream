namespace ToyStream.Protocol.Send;

using Messages.Send;
using Shared;

public readonly struct ShortValue: IProtocolComponent
{
    private readonly short value;

    public ShortValue(short value)
    {
        this.value = value;
    }

    public int Size => sizeof(short);

    public short Value => this.value;
    
    public void Write(ref ProtocolPrimitiveWriter primitiveWriter)
    {
        primitiveWriter.Write(this.value);
    }
}