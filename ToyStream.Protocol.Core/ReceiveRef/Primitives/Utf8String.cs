namespace ToyStream.Protocol.ReceiveRef;

using System.Buffers;
using System.Diagnostics.CodeAnalysis;
using System.Text;

public readonly struct Utf8String: IProtocolComponent<Utf8String>
{
    private readonly ReadOnlySequence<byte> buffer;

    private Utf8String(ReadOnlySequence<byte> buffer)
    {
        this.buffer = buffer;
    }

   
    public string Value => Encoding.UTF8.GetString(this.buffer);

    public static bool TryRead(ref SequenceReader<byte> reader, [NotNullWhen(true)] out Utf8String? component, out int bytesToRead)
    {
        // Copy reader so we don't advance if we don't have enough to read.
        var tryReader = reader;

        //Read length
        if (!ShortValue.TryRead(ref tryReader, out var length, out bytesToRead))
        {
            component = default;
            return false;
        }

        bytesToRead =  length.Value.Value - (int) tryReader.Remaining;
        // See if we have enough bytes to read the whole string.
        if (bytesToRead > 0)
        {
            component = default;
            return false;
        }
        bytesToRead = 0;

        component = new Utf8String(tryReader.UnsafeReadExact(length.Value.Value));
        // Success, set updated reader
        reader = tryReader;
        return true;
    }
}