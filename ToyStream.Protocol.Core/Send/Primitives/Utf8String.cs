namespace ToyStream.Protocol.Send;

using System.Text;
using ToyStream.Shared;

public readonly ref struct Utf8String: IProtocolComponent
{
    private readonly string value;
    private readonly short strLength;

    public Utf8String(string value)
    {
        ArgumentOutOfRangeException.ThrowIfGreaterThan(value.Length, 64);
        this.value = value;
        this.strLength = (short) Encoding.UTF8.GetByteCount(this.value); 
    }

    public int Size => this.strLength + sizeof(short);

    public void Write(ref SpanWriter writer)
    {
        writer.Write(this.strLength);
        writer.Advance(Encoding.UTF8.GetBytes(this.value, writer.Remaining));
    }
}