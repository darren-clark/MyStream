namespace ToyStream.Protocol.Send;

using Messages.Send;
using Shared;

public interface IProtocolComponent
{
    public int Size { get; }
    public void Write(ref ProtocolPrimitiveWriter primitiveWriter);
}