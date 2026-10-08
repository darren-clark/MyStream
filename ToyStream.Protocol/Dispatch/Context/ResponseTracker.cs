namespace ToyStream.Protocol.Dispatch.Context;

using System.IO.Pipelines;
using System.Runtime.CompilerServices;
using Messages;
using Responses;

/// <summary>
/// The outbound half: owns the pending-response slot so that both the caller sending a request and
/// the context completing it can reach the same completion source without the context needing a
/// reference back to the connection.
/// </summary>
public sealed class ResponseTracker(PipeWriter writer)
{
    private readonly ResponseCompletionSourceCache cachedCompletionSources = new();

    private ResponseCompletionSource? responseCompletionSource;

    public ValueTask<TResponse> SendMessage<TRequestMessage, TResponseMessage, TResponse>(
        TRequestMessage requestMessage, CancellationToken cancellationToken)
        where TRequestMessage : struct, IRequestMessage<TRequestMessage, TResponseMessage>, allows ref struct
        where TResponseMessage : struct, IResponseMessage<TResponseMessage>, allows ref struct
        where TResponse : class, IResponse<TResponseMessage, TResponse>
    {
        var completionSource = this.cachedCompletionSources.Get<TResponse>();
        this.responseCompletionSource = completionSource;

        var primitiveWriter = new PrimitiveWriter<PipeWriter>(writer);
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

    public void CompleteResponse<TResponseMessage, TResponse>(ref readonly TResponseMessage message)
        where TResponseMessage : struct, IResponseMessage<TResponseMessage>, allows ref struct
        where TResponse : class, IResponse<TResponseMessage, TResponse>
        => this.responseCompletionSource!.TrySetResult(TResponse.CreateResponse(in message));

    public void CompleteResponse<TResponse>(TResponse response)
    {
        this.responseCompletionSource!.TrySetResult(response);
    }
}
