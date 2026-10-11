namespace ToyStream.Protocol;

using System.Buffers;
using System.Diagnostics;
using System.Runtime.CompilerServices;
using Shared;

[method: MethodImpl(MethodImplOptions.AggressiveInlining)]
public ref struct PrimitiveReader(ReadOnlySequence<byte> sequence) : IPrimitiveReader
{
    private SequenceReader<byte> reader = new(sequence);

    public ReadOnlySequence<byte> Remaining => this.reader.UnreadSequence;

    public long Consumed => this.reader.Consumed;

    [MethodImpl(MethodImplOptions.AggressiveInlining)]
    public bool TryReadInt16(out short value, out int bytesToRead)
    {
        if (reader.Remaining < sizeof(short))
        {
            value = default;
            bytesToRead = (int)(sizeof(short) - reader.Remaining);
            return false;
        }

        var read = this.reader.TryReadBigEndian(out value);
        Debug.Assert(read);
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

        var read = this.reader.TryReadExact(length, out var buffer);
        Debug.Assert(read);
        value = new RefUtf8String(buffer);
        bytesToRead = 0;
        return true;
    }

    [MethodImpl(MethodImplOptions.AggressiveInlining)]
    public bool TryReadMessageType(out MessageType messageType, out int bytesToRead)
    {
        if (reader.Remaining < sizeof(byte))
        {
            messageType = default;
            bytesToRead = 1;
            return false;
        }

        var read = this.reader.TryRead(out var value);
        Debug.Assert(read);
        messageType = (MessageType)value;
        bytesToRead = 0;
        return true;
    }
}