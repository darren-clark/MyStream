namespace ToyStream.Client.Protocol;

using Shared;

public interface IClientMessage
{
    public int Length { get; }
    public void Write(ref SpanWriter writer);
}