using System.Diagnostics;
using System.Diagnostics.CodeAnalysis;

namespace ToyStream.Protocol;

internal static class ThrowHelpers
{
    [DoesNotReturn]
    [StackTraceHidden]
    public static void ThrowInvalidEnumSizeException<TEnum>() where TEnum : unmanaged, Enum
        => throw new InvalidOperationException($"Invalid enum size for type {typeof(TEnum)}");
}