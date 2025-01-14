namespace ToyStream.Protocol.ReceiveRef;

using System.Buffers;
using System.Diagnostics.CodeAnalysis;
using System.Text;

public readonly ref struct Utf8StringValue: IProtocolComponent<Utf8StringValue>
{
    private readonly ReadOnlySequence<byte> buffer;

   
    private Utf8StringValue(ReadOnlySequence<byte> buffer)
    {
        this.buffer = buffer;
    }
  
    public string Value => Encoding.UTF8.GetString(this.buffer);

    public static bool TryRead(scoped ref SequenceReader<byte> reader, [NotNullWhen(true)] out Utf8StringValue component, out int bytesToRead)
    {
        // Copy reader so we don't advance if we don't have enough to read.
        var tryReader = reader;

        //Read length
        if (!ShortValue.TryRead(ref tryReader, out var length, out bytesToRead))
        {
            component = default;
            return false;
        }

        bytesToRead =  length.Value - (int) tryReader.Remaining;
        // See if we have enough bytes to read the whole string.
        if (bytesToRead > 0)
        {
            component = default;
            return false;
        }
        bytesToRead = 0;

        component = new Utf8StringValue(tryReader.UnsafeReadExact(length.Value));
        // Success, set updated reader
        reader = tryReader;
        return true;
    }
}