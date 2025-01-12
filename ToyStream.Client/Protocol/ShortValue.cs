namespace ToyStream.Client.Protocol;

using Shared;

public ref struct ShortValue
{
    private short value;

    public ShortValue(short value)
    {
        this.value = value;
    }

    public int Length { get; } = sizeof(short);

    public readonly void Write(ref SpanWriter writer)
    {
        writer.Write(this.value);
    }
}