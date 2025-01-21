namespace ToyStream.Protocol.Send;

using System.Diagnostics;
using System.Text;
using Messages.Send;
using Shared;

[DebuggerDisplay($"{Encoding.UTF8.GetString(value)}")]
public readonly ref struct Utf8StringValue: IProtocolComponent
{
    private readonly ReadOnlySpan<byte> value;
    // private readonly string value;
    // private readonly ShortValue length;

    public Utf8StringValue(string value)
    {
        ArgumentOutOfRangeException.ThrowIfGreaterThan(value.Length, 64);
        this.value = Encoding.UTF8.GetBytes(value);
        // this.value = value;
        // this.length = new ShortValue((short) Encoding.UTF8.GetByteCount(this.value)); 
    }

    public Utf8StringValue(ReadOnlySpan<byte> value)
    {
        ArgumentOutOfRangeException.ThrowIfGreaterThan(value.Length, 64);
        this.value = value;
    }

    public int Size => this.value.Length + sizeof(short);

    public void Write(ref ProtocolPrimitiveWriter primitiveWriter)
    {
        primitiveWriter.Write(this.length.Value);
        primitiveWriter.Advance(Encoding.UTF8.GetBytes(this.value, primitiveWriter.Remaining));
    }
}