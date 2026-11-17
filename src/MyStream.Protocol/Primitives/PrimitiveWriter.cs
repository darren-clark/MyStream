using System.Buffers;
using System.Buffers.Binary;
using System.Diagnostics;
using System.IO.Pipelines;
using System.Runtime.CompilerServices;

namespace MyStream.Protocol.Primitives;

/// <summary>
/// A struct rather than a class: <c>IMessage.Write</c> is generic over the writer, and a reference type
/// argument collapses every instantiation into one shared __Canon body whose calls cannot devirtualize.
/// Unlike <see cref="PrimitiveReader"/> it holds no spans, so it need not be a ref struct.
/// </summary>
public readonly struct PrimitiveWriter(PipeWriter writer) : IPrimitiveWriter
{
    [MethodImpl(MethodImplOptions.AggressiveInlining)]
    public void Write(byte value)
    {
        this.Write(new ReadOnlySpan<byte>(ref value));
    }

    [MethodImpl(MethodImplOptions.AggressiveInlining)]
    public void Write(short value)
    {
        Span<byte> span = stackalloc byte[sizeof(short)];
        BinaryPrimitives.WriteInt16BigEndian(span, value);
        this.Write(span);
    }

    [MethodImpl(MethodImplOptions.AggressiveInlining)]
    public void Write(ushort value)
    {
        Span<byte> span = stackalloc byte[sizeof(ushort)];
        BinaryPrimitives.WriteUInt16BigEndian(span, value);
        writer.Write(span);
    }

    [MethodImpl(MethodImplOptions.AggressiveInlining)]
    public void Write(int value)
    {
        Span<byte> buffer = stackalloc byte[sizeof(int)];
        BinaryPrimitives.WriteInt32BigEndian(buffer, value);
        writer.Write(buffer);
    }

    [MethodImpl(MethodImplOptions.AggressiveInlining)]
    public void Write(long value)
    {
        Span<byte> buffer = stackalloc byte[sizeof(long)];
        BinaryPrimitives.WriteInt64BigEndian(buffer, value);
        writer.Write(buffer);
    }

    [MethodImpl(MethodImplOptions.AggressiveInlining)]
    public void Write(ReadOnlySpan<byte> value) =>  writer.Write(value);
    
    [MethodImpl(MethodImplOptions.AggressiveInlining)]
    public void Write(ReadOnlySequence<byte> value)
    {
        if (value.IsSingleSegment)
        {
            this.Write(value.FirstSpan);
            return;
        }

        foreach (var segment in value)
        {
            this.Write(segment.Span);
        }
    }

    // /// <summary>Transcodes UTF-16 source straight into the output buffer, so no scratch buffer is needed.</summary>
    // [MethodImpl(MethodImplOptions.AggressiveInlining)]
    // public void Write(RefUtf8String value)
    // {
    //     var byteCount = value.GetByteCount();
    //
    //     // The length goes on the wire as a short, so an oversized string would wrap to negative.
    //     if ((uint)byteCount > ProtocolConstants.MaxStringSize)
    //     {
    //         ThrowHelpers.ThrowInvalidLength(byteCount, ProtocolConstants.MaxStringSize);
    //     }
    //
    //     this.Write((short) byteCount);
    //
    //     var span = writer.GetSpan(byteCount);
    //     Debug.Assert(span.Length >= byteCount, "GetSpan must honour the size hint");
    //     writer.Advance(value.CopyTo(span));
    // }

    /// <summary>A length-prefixed run of opaque bytes, bounded by the caller's limit.</summary>
    // [MethodImpl(MethodImplOptions.AggressiveInlining)]
    // public void Write(RefBytes value, int maximum)
    // {
    //     var length = value.Length;
    //     if ((uint)length > (uint)maximum)
    //     {
    //         ThrowHelpers.ThrowInvalidLength(length, maximum);
    //     }
    //
    //     this.Write(length);
    //
    //     var span = writer.GetSpan(length);
    //     Debug.Assert(span.Length >= length, "GetSpan must honour the size hint");
    //     writer.Advance(value.CopyTo(span));
    // }

    [MethodImpl(MethodImplOptions.AggressiveInlining)]
    public void WriteEnum<TEnum>(TEnum value) where TEnum: unmanaged, Enum
    {
        var size = Unsafe.SizeOf<TEnum>();
        switch (size)
        {
            case 1:
                this.Write(Unsafe.As<TEnum, byte>(ref value));
                break;
            case 2:
                this.Write(Unsafe.As<TEnum, short>(ref value));
                break;
            case 4:
                this.Write(Unsafe.As<TEnum, int>(ref value));
                break;
            case 8:
                this.Write(Unsafe.As<TEnum, long>(ref value));
                break;
            default:
                ThrowHelpers.ThrowInvalidEnumSizeException<TEnum>();
                break;
        }
    }
    
    [MethodImpl(MethodImplOptions.AggressiveInlining)]
    public void Write<T>(T value) where T : struct, IProtocolPrimitive<T>, allows ref struct
    {
        value.Write(this);
    }
}
