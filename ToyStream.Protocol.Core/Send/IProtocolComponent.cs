namespace ToyStream.Protocol.Send;

using Shared;

public interface IProtocolComponent
{
    public int Size { get; }
    public void Write(ref BigEndianSpanWriter writer);
}