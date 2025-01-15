namespace ToyStream.Protocol;

using System.Buffers;
using System.Buffers.Binary;
using System.Diagnostics;
using System.Diagnostics.CodeAnalysis;
using System.Diagnostics.Contracts;
using System.Runtime.CompilerServices;

public static class SequenceExtensions
{
    [MethodImpl(MethodImplOptions.AggressiveInlining)]
    private static short UnsafeReadInt16BigEndian(ref this SequenceReader<byte> reader)
    {
        Debug.Assert(reader.TryReadBigEndian(out short value), message: "Size precondition should have been checked");
        return value;
    }

    [MethodImpl(MethodImplOptions.AggressiveInlining)]
    private static ReadOnlySequence<byte> UnsafeReadExact(ref this SequenceReader<byte> reader, int count)
    {
        Debug.Assert(reader.TryReadExact(count, out var result));
        return result;
    }

    [MethodImpl(MethodImplOptions.AggressiveInlining)]
    private static void UnsafeCopyTo(ref this SequenceReader<byte> reader, Span<byte> destination)
    {
        Debug.Assert(reader.TryCopyTo(destination), "Size precondition should have been checked");
        reader.Advance(destination.Length);
    }

    [MethodImpl(MethodImplOptions.AggressiveInlining)]
    public static bool TryReadInt16(ref this SequenceReader<byte> reader, [NotNullWhen(true)] out short? value,
        out int bytesToRead)
    {
        if (reader.Remaining < sizeof(short))
        {
            value = default;
            bytesToRead = (int)(sizeof(short) - reader.Remaining);
            return false;
        }

        value = reader.UnsafeReadInt16BigEndian();
        bytesToRead = 0;
        return true;
    }
}