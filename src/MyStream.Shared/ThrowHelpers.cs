using System.Diagnostics;
using System.Diagnostics.CodeAnalysis;

namespace MyStream;

public static class ThrowHelpers
{
    [StackTraceHidden]
    [DoesNotReturn]
    public static T ThrowNotImplemented<T>(string? message = null) => throw new NotImplementedException(message);

    [StackTraceHidden]
    [DoesNotReturn]
    public static T ThrowNotImplemented<T>(string message, Exception innerException) =>
        throw new NotImplementedException(message, innerException);

    [StackTraceHidden]
    [DoesNotReturn]
    public static void ThrowInvalidOperation(string? message = null) => throw new InvalidOperationException(message);

    [StackTraceHidden]
    [DoesNotReturn]
    public static void ThrowInvalidOperation(string? message, Exception innerException) =>
        throw new InvalidOperationException(message, innerException);

    [StackTraceHidden]
    [DoesNotReturn]
    public static T ThrowInvalidOperation<T>(string? message = null) => throw new InvalidOperationException(message);

    [StackTraceHidden]
    [DoesNotReturn]
    public static T ThrowInvalidOperation<T>(string? message, Exception innerException) =>
        throw new InvalidOperationException(message, innerException);
    
    [StackTraceHidden]
    [DoesNotReturn]
    public static T ThrowArgumentOutOfRangeException<T>(string? message = null) => throw new ArgumentOutOfRangeException(message);
}