using System.Diagnostics.CodeAnalysis;

namespace ToyStream.Protocol;

using System.Buffers;
using System.Buffers.Binary;
using System.Diagnostics;
using System.IO.Pipelines;
using System.Runtime.CompilerServices;
using ToyStream.Shared;

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
    public void Write(int value)
    {
        Span<byte> buffer = stackalloc byte[sizeof(int)];
        BinaryPrimitives.WriteInt32BigEndian(buffer, value);
        this.Write(buffer);
    }

    [MethodImpl(MethodImplOptions.AggressiveInlining)]
    public void Write(long value)
    {
        Span<byte> buffer = stackalloc byte[sizeof(long)];
        BinaryPrimitives.WriteInt64BigEndian(buffer, value);
        this.Write(buffer);
    }

    /// <summary>
    /// <see cref="IBufferWriter{T}"/>'s Write is an extension on the interface, so routing through it
    /// would box the writer. GetSpan/Advance are called on PipeWriter directly instead.
    /// Requires a contiguous span, unlike the extension, which segments large writes.
    /// </summary>
    [MethodImpl(MethodImplOptions.AggressiveInlining)]
    private void Write(ReadOnlySpan<byte> value)
    {
        var span = writer.GetSpan(value.Length);
        Debug.Assert(span.Length >= value.Length, "GetSpan must honour the size hint");

        value.CopyTo(span);
        writer.Advance(value.Length);
    }

    [MethodImpl(MethodImplOptions.AggressiveInlining)]
    private void Write(ReadOnlySequence<byte> value)
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

    [MethodImpl(MethodImplOptions.AggressiveInlining)]
    public void Write(RefUtf8String value)
    {
        this.Write(value.Length);
        this.Write(value.Sequence);
    }

    [MethodImpl(MethodImplOptions.AggressiveInlining)]
    public void Write<TEnum>(TEnum value) where TEnum: unmanaged, Enum
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
}
