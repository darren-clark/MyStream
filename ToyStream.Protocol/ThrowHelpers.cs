using System.Diagnostics;
using System.Diagnostics.CodeAnalysis;

namespace ToyStream.Protocol;

internal static class ThrowHelpers
{
    [DoesNotReturn]
    [StackTraceHidden]
    public static void ThrowInvalidEnumSizeException<TEnum>() where TEnum : unmanaged, Enum
        => throw new InvalidOperationException($"Invalid enum size for type {typeof(TEnum)}");

    [DoesNotReturn]
    [StackTraceHidden]
    public static void ThrowInvalidLength(int length, int maximum)
        => throw new InvalidOperationException($"Declared length {length} is outside the allowed range 0 to {maximum}.");
}