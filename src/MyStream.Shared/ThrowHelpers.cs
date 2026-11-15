using System.Diagnostics;

namespace MyStream;

public static class ThrowHelpers
{
    [StackTraceHidden]
    public static T ThrowNotImplemented<T>(string? message = null) => throw new NotImplementedException(message);

    [StackTraceHidden]
    public static T ThrowNotImplemented<T>(string message, Exception innerException) =>
        throw new NotImplementedException(message, innerException);

    [StackTraceHidden]
    public static void ThrowInvalidOperation(string? message = null) => throw new InvalidOperationException(message);

    [StackTraceHidden]
    public static void ThrowInvalidOperation(string? message, Exception innerException) =>
        throw new InvalidOperationException(message, innerException);

    [StackTraceHidden]
    public static T ThrowInvalidOperation<T>(string? message = null) => throw new InvalidOperationException(message);

    [StackTraceHidden]
    public static T ThrowInvalidOperation<T>(string? message, Exception innerException) =>
        throw new InvalidOperationException(message, innerException);
}