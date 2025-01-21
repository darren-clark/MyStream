namespace ToyStream.Protocol;

using System.Buffers;
using System.Diagnostics;
using System.Runtime.CompilerServices;
using Shared;

public ref struct ProtocolPrimitiveReader
{
    private SequenceReader<byte> reader;

    [MethodImpl(MethodImplOptions.AggressiveInlining)]
    public ProtocolPrimitiveReader(SequenceReader<byte> reader)
    {
        this.reader = reader;
    }

    [MethodImpl(MethodImplOptions.AggressiveInlining)]
    public bool TryReadInt16(out short value, out int bytesToRead)
    {
        if (reader.Remaining < sizeof(short))
        {
            value = default;
            bytesToRead = (int)(sizeof(short) - reader.Remaining);
            return false;
        }

        Debug.Assert(this.reader.TryReadBigEndian(out value));
        bytesToRead = 0;
        return true;
    }

    [MethodImpl(MethodImplOptions.AggressiveInlining)]
    public bool TryReadUtf8String(out RefUtf8String value, out int bytesToRead)
    {
        if (!TryReadInt16(out var length, out bytesToRead))
        {
            value = default;
            return false;
        }

        if (reader.Remaining < length)
        {
            value = default;
            bytesToRead = (int)(length - reader.Remaining);
            return false;
        }

        Debug.Assert(this.reader.TryReadExact(length, out var buffer));
        value = new RefUtf8String(buffer);
        bytesToRead = 0;
        return true;
    }    
}