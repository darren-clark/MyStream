namespace ToyStream.Protocol;

using System.Diagnostics;
using System.Diagnostics.CodeAnalysis;
using System.IO.Pipelines;
using System.Runtime.CompilerServices;
using Messages;
using Responses;

/// <summary>
/// The outbound half, and the half that does not depend on the handler: sending requests and
/// completing the responses to them. Both roles send, so this is deliberately not role-specific.
/// </summary>
public abstract class Connection(PipeWriter writer)
{
    /// <summary>A static method group, so the conversion is cached rather than allocated per send.</summary>
    private delegate DispatchResult ResponseParser(Connection connection, ResponseType responseType,
        ref PrimitiveReader reader);

    private readonly ResponseCompletionSourceCache cachedCompletionSources = new();

    private ResponseCompletionSource? responseCompletionSource;
    private ResponseParser? parser;

    protected PipeWriter Writer => writer;

    public ValueTask<TResponse> SendMessage<TRequestMessage, TResponseMessage, TResponse>(
        TRequestMessage requestMessage, CancellationToken cancellationToken)
        where TRequestMessage : struct, IRequestMessage<TRequestMessage, TResponseMessage>, allows ref struct
        where TResponseMessage : struct, IResponseMessage<TResponseMessage>, allows ref struct
        where TResponse : class, IResponse<TResponseMessage, TResponse>
    {
        var completionSource = this.cachedCompletionSources.Get<TResponse>();
        this.responseCompletionSource = completionSource;
        this.parser = ParseAndComplete<TResponseMessage, TResponse>;

        var primitiveWriter = new PrimitiveWriter(writer);
        primitiveWriter.Write(TRequestMessage.Type);
        requestMessage.Write(primitiveWriter);

        return SendAndWait(
            writer.FlushAsync(cancellationToken),
            completionSource,
            cancellationToken.Register(static c => ((ResponseCompletionSource<TResponse>)c!).Cancel(), completionSource));

        [AsyncMethodBuilder(typeof(PoolingAsyncValueTaskMethodBuilder<>))]
        static async ValueTask<TResponse> SendAndWait(ValueTask<FlushResult> writerTask,
            ResponseCompletionSource<TResponse> responseCompletion, CancellationTokenRegistration cancellation)
        {
            await writerTask.ConfigureAwait(false);
            var result = await responseCompletion.ValueTask.ConfigureAwait(false);
            await cancellation.DisposeAsync();
            return result;
        }
    }

    /// <summary>
    /// A response only ever answers a request this side sent, so the outstanding request picks the
    /// decoder and the type byte is checked against it rather than switched on.
    /// </summary>
    private protected DispatchResult TryParseAndComplete(ResponseType responseType, ref PrimitiveReader reader)
    {
        if (this.parser is null)
        {
            ThrowUnsolicited(responseType);
        }

        return this.parser(this, responseType, ref reader);
    }

    private static DispatchResult ParseAndComplete<TResponseMessage, TResponse>(Connection connection,
        ResponseType responseType, ref PrimitiveReader reader)
        where TResponseMessage : struct, IResponseMessage<TResponseMessage>, allows ref struct
        where TResponse : class, IResponse<TResponseMessage, TResponse>
    {
        if (responseType != TResponseMessage.Type)
        {
            ThrowWrongResponse(responseType, TResponseMessage.Type);
        }

        if (!TResponseMessage.TryRead(ref reader, out var response, out var bytesNeeded))
        {
            return DispatchResult.Insufficient(bytesNeeded);
        }

        connection.parser = null;
        connection.responseCompletionSource!.TrySetResult(TResponse.CreateResponse(in response));
        return DispatchResult.Dispatched(default, wroteResponse: false);
    }

    [DoesNotReturn]
    [StackTraceHidden]
    private static void ThrowUnsolicited(ResponseType responseType) =>
        throw new InvalidOperationException($"Received {responseType} with no request outstanding.");

    [DoesNotReturn]
    [StackTraceHidden]
    private static void ThrowWrongResponse(ResponseType received, ResponseType expected) =>
        throw new InvalidOperationException($"Received {received} while awaiting {expected}.");
}
