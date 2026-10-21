namespace ToyStream.Protocol;

using System.Buffers;
using System.Diagnostics;
using System.Runtime.CompilerServices;
using System.Runtime.InteropServices;
using Shared;

[method: MethodImpl(MethodImplOptions.AggressiveInlining)]
public ref struct PrimitiveReader(ReadOnlySequence<byte> sequence) : IPrimitiveReader
{
    private SequenceReader<byte> reader = new(sequence);

    public ReadOnlySequence<byte> Remaining => this.reader.UnreadSequence;

    public long Consumed => this.reader.Consumed;

    [MethodImpl(MethodImplOptions.AggressiveInlining)]
    public bool TryReadByte(out byte value, out int bytesToRead)
    {
        if (reader.Remaining < sizeof(byte))
        {
            value = default;
            bytesToRead = (int)(sizeof(byte) - reader.Remaining);
            return false;
        }

        var read = this.reader.TryRead(out value);
        Debug.Assert(read);
        bytesToRead = 0;
        return true;
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

        var read = this.reader.TryReadBigEndian(out value);
        Debug.Assert(read);
        bytesToRead = 0;
        return true;
    }
    
    [MethodImpl(MethodImplOptions.AggressiveInlining)]
    public bool TryReadInt32(out int value, out int bytesToRead)
    {
        if (reader.Remaining < sizeof(int))
        {
            value = default;
            bytesToRead = (int)(sizeof(int) - reader.Remaining);
            return false;
        }

        var read = this.reader.TryReadBigEndian(out value);
        Debug.Assert(read);
        bytesToRead = 0;
        return true;
    }

    [MethodImpl(MethodImplOptions.AggressiveInlining)]
    public bool TryReadInt64(out long value, out int bytesToRead)
    {
        if (reader.Remaining < sizeof(long))
        {
            value = default;
            bytesToRead = (int)(sizeof(long) - reader.Remaining);
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

        // Unsigned so a negative length is rejected by the same comparison as an oversized one.
        if ((uint)length > ProtocolConstants.MaxStringSize)
        {
            ThrowHelpers.ThrowInvalidLength(length, ProtocolConstants.MaxStringSize);
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
    
    /// <summary>A length-prefixed run of opaque bytes, bounded by the caller's limit.</summary>
    [MethodImpl(MethodImplOptions.AggressiveInlining)]
    public bool TryReadBytes(int maximum, out RefBytes value, out int bytesToRead)
    {
        if (!TryReadInt32(out var length, out bytesToRead))
        {
            value = default;
            return false;
        }

        // Unsigned so a negative length is rejected by the same comparison as an oversized one.
        if ((uint)length > (uint)maximum)
        {
            ThrowHelpers.ThrowInvalidLength(length, maximum);
        }

        if (reader.Remaining < length)
        {
            value = default;
            bytesToRead = (int)(length - reader.Remaining);
            return false;
        }

        var read = this.reader.TryReadExact(length, out var buffer);
        Debug.Assert(read);
        value = new RefBytes(buffer);
        bytesToRead = 0;
        return true;
    }

    /// <summary>A run of bytes whose length the caller already knows, left as a view over the buffer.</summary>
    [MethodImpl(MethodImplOptions.AggressiveInlining)]
    public bool TryReadExact(int length, out ReadOnlySequence<byte> value, out int bytesToRead)
    {
        if (reader.Remaining < length)
        {
            value = default;
            bytesToRead = (int)(length - reader.Remaining);
            return false;
        }

        var read = this.reader.TryReadExact(length, out value);
        Debug.Assert(read);
        bytesToRead = 0;
        return true;
    }

    [MethodImpl(MethodImplOptions.AggressiveInlining)]
    public bool TryReadEnum<T>(out T value, out int bytesToRead) where T : unmanaged, Enum
    {
        var size = Unsafe.SizeOf<T>();
        value = default;
        switch (size)
        {
            case 1:
                if (!TryReadByte(out var byteValue, out bytesToRead)) return false;
                value=Unsafe.As<byte, T>(ref byteValue);
                break;
            case 2:
                if (!TryReadInt16(out var shortValue, out bytesToRead)) return false;
                value=Unsafe.As<short, T>(ref shortValue);
                break;
            case 4:
                if (!TryReadInt32(out var intValue, out bytesToRead)) return false;
                value=Unsafe.As<int, T>(ref intValue);
                break;
            case 8:
                if (!TryReadInt64(out var longValue, out bytesToRead)) return false;
                value=Unsafe.As<long, T>(ref longValue);
                break;
            default:
                bytesToRead = 0;
                ThrowHelpers.ThrowInvalidEnumSizeException<T>();
                break;
        }
        
        return true;
    }
}
