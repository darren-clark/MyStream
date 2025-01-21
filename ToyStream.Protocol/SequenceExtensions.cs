// namespace ToyStream.Protocol;
//
// using System.Buffers;
// using System.Buffers.Binary;
// using System.Diagnostics;
// using System.Diagnostics.Contracts;
// using System.Runtime.CompilerServices;
// using Shared;
//
// public static class SequenceExtensions
// {
//     [MethodImpl(MethodImplOptions.AggressiveInlining)]
//     [Pure]
//     public static short ReadInt16BigEndian(ref readonly this ReadOnlySequence<byte> buffer)
//     {
//         if (buffer.IsSingleSegment)
//         {
//             return BinaryPrimitives.ReadInt16BigEndian(buffer.First.Span);
//         }
//         
//         Span<byte> shortBuffer = stackalloc byte[sizeof(short)];
//         buffer.CopyTo(shortBuffer);
//         return BinaryPrimitives.ReadInt16BigEndian(shortBuffer);
//     }
//     
//     [MethodImpl(MethodImplOptions.AggressiveInlining)]
//     public static short UnsafeReadInt16BigEndian(ref this SequenceReader<byte> reader)
//     {
//         Debug.Assert(reader.TryReadBigEndian(out short value), message: "Size precondition should have been checked");
//         return value;
//     }
//
//     [MethodImpl(MethodImplOptions.AggressiveInlining)]
//     public static ReadOnlySequence<byte> UnsafeReadExact(ref this SequenceReader<byte> reader, int count)
//     {
//         Debug.Assert(reader.TryReadExact(count, out var result));
//         return result;
//     }
//     
//     [MethodImpl(MethodImplOptions.AggressiveInlining)]
//     public static void UnsafeCopyTo(ref this SequenceReader<byte> reader, Span<byte> destination)
//     {
//         Debug.Assert(reader.TryCopyTo(destination), "Size precondition should have been checked");
//         reader.Advance(destination.Length);
//     }
//     
//     [MethodImpl(MethodImplOptions.AggressiveInlining)]
//     public static bool TryReadInt16(ref this SequenceReader<byte> reader, out short value, out int bytesToRead)
//     {
//         if (reader.Remaining < sizeof(short))
//         {
//             value = default;
//             bytesToRead = (int)(sizeof(short) - reader.Remaining);
//             return false;
//         }
//
//         value = reader.UnsafeReadInt16BigEndian();
//         bytesToRead = 0;
//         return true;
//     }
//     
//     [MethodImpl(MethodImplOptions.AggressiveInlining)]
//     public static bool TryReadUtf8String(this scoped ref SequenceReader<byte> reader, out RefUtf8String value, out int bytesToRead)
//     {
//         if (!TryReadInt16(ref reader, out var length, out bytesToRead))
//         {
//             value = default;
//             return false;
//         }
//
//         if (reader.Remaining < length)
//         {
//             value = default;
//             bytesToRead = (int)(length - reader.Remaining);
//             return false;
//         }
//
//         value = new RefUtf8String(reader.UnsafeReadExact(length));
//         bytesToRead = 0;
//         return true;
//     }
// }