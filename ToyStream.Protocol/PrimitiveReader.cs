namespace ToyStream.Protocol;

using System.Buffers;
using System.Diagnostics;
using System.Runtime.CompilerServices;
using Shared;

using static Shared.ThrowHelpers;

[method: MethodImpl(MethodImplOptions.AggressiveInlining)]
internal ref struct PrimitiveReader(ReadOnlySequence<byte> sequence) : IPrimitiveReader
{
    private SequenceReader<byte> reader = new(sequence);

    public ReadOnlySequence<byte> Remaining => this.reader.UnreadSequence;

    public long Consumed => this.reader.Consumed;

    [method: MethodImpl(MethodImplOptions.AggressiveInlining)]
    public bool TryReadEnum<TEnum>(out TEnum value, out int bytesToRead) where TEnum : struct,Enum
    {
        var enumSize = Unsafe.SizeOf<TEnum>();

        // make compiler happy since it doesn't know that
        // the throwhelper method will never return
        bytesToRead = 0;
        value = enumSize switch
        {
            1 => TryReadByte(out var b, out bytesToRead) ? Unsafe.As<byte, TEnum>(ref b) : default,
            2 => TryReadInt16(out var s, out bytesToRead) ? Unsafe.As<short, TEnum>(ref s) : default,
            4 => TryReadInt32(out var i, out bytesToRead) ? Unsafe.As<int, TEnum>(ref i) : default,
            8 => TryReadInt64(out var l, out bytesToRead) ? Unsafe.As<long, TEnum>(ref l) : default,
            _ => ThrowInvalidOperation<TEnum>("Unexpected enum size")
        };

        return bytesToRead == 0;
    }

    
    public bool TryReadByte(out byte value, out int bytesToRead)
    {
        if (this.reader.Remaining  == 0)
        {
            bytesToRead = 1;
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
        if (this.reader.Remaining < sizeof(int))
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
        if (this.reader.Remaining < sizeof(long))
        {
            value = default;
            bytesToRead = (int)(sizeof(long) - reader.Remaining);
            return false;
        }
        
        var read= this.reader.TryReadBigEndian(out value);
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
}