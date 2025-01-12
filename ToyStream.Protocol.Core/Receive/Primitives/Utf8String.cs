namespace ToyStream.Protocol.Receive;

using System.Buffers;
using System.Diagnostics.CodeAnalysis;
using System.Text;

public readonly struct Utf8String: IDisposable, IProtocolComponent<Utf8String>
{
    private readonly byte[] buffer;
    private readonly ShortValue length;

    private Utf8String(byte[] value, short length)
    {
        this.buffer = value;
        this.length = length;
    }

    public Utf8String(string value)
    {
        this.length = (short) Encoding.UTF8.GetByteCount(value);
        this.buffer = ArrayPool<byte>.Shared.Rent(this.length);
        Encoding.UTF8.GetBytes(value, new Span<byte>(this.buffer, 0, this.length));
    }
    
    public string Value => Encoding.UTF8.GetString(new ReadOnlySpan<byte>(this.buffer, 0, this.length));

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

        bytesToRead =  length.Value - (int) tryReader.Remaining;
        // See if we have enough bytes to read the whole string.
        if (bytesToRead > 0)
        {
            component = default;
            return false;
        }
        bytesToRead = 0;
        
        // Get buffer for string, MAY BE LONGER THAN REQUESTED.
        var stringBuffer = ArrayPool<byte>.Shared.Rent(length.Value);
        tryReader.UnsafeCopyTo(new Span<byte>(stringBuffer, 0, length.Value));

        // Success, set updated reader
        reader = tryReader;
        component = new Utf8String(stringBuffer, length.Value);
        return true;
    }
    
    public void Dispose()
    {
        ArrayPool<byte>.Shared.Return(this.buffer);
    }

}