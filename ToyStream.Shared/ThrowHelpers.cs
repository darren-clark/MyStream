namespace ToyStream.Shared;

using System.Diagnostics;
using System.Diagnostics.CodeAnalysis;

public static class ThrowHelpers
{

    [StackTraceHidden]
    public static T ThrowNotImplemented<T>(string? message = null) => message == null
        ? throw new NotImplementedException()
        : throw new NotImplementedException(message);

    [StackTraceHidden]
    public static void ThrowNotImplemented(string? message = null)
    {
        if (message == null)
        {
            throw new NotImplementedException();
        }
        throw new NotImplementedException(message);
    }
    
    [StackTraceHidden]
    public static T ThrowNotImplemented<T>(string message, Exception innerException) =>
        throw new NotImplementedException(message, innerException);
    
    [StackTraceHidden]
    [DoesNotReturn]
    public static T ThrowInvalidOperation<T>(string? message = null) => message == null
        ? throw new InvalidOperationException()
        : throw new InvalidOperationException(message);

    [StackTraceHidden]
    public static T ThrowInvalidOperation<T>(string message, Exception innerException) =>
        throw new InvalidOperationException(message, innerException);
    
    
}