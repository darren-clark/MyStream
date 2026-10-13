namespace ToyStream.Protocol;

using System.Buffers;
using System.Diagnostics;
using System.Diagnostics.CodeAnalysis;
using System.IO.Pipelines;
using System.Runtime.CompilerServices;

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
   
    public ValueTask SendMessage<TRequestMessage>(TRequestMessage requestMessage, CancellationToken cancellationToken)
        where TRequestMessage : struct, IRequestMessage<TRequestMessage>, allows ref struct
    {
        var primitiveWriter = new PrimitiveWriter(writer);
        primitiveWriter.Write(TRequestMessage.Type);
        requestMessage.Write(primitiveWriter);
        
        return Flush(cancellationToken);
    }
    
    public ValueTask SendStreamingMessage<TRequestMessage>(TRequestMessage requestMessage, CancellationToken cancellationToken)
        where TRequestMessage : struct, IStreamingRequestMessage<TRequestMessage>, allows ref struct
    {
        var primitiveWriter = new PrimitiveWriter(writer);
        primitiveWriter.Write(TRequestMessage.Type);
        requestMessage.Write(primitiveWriter);
        
        return StreamMessage(requestMessage.Size, requestMessage.Body, cancellationToken);
    }
    
    
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


    }

    /// <summary>
    /// Header then payload, so a large body is never buffered whole. The header write stays here
    /// because a ref struct cannot be a parameter of an async method; only the copy and the wait are.
    /// </summary>
    public ValueTask<TResponse> SendStreamingMessage<TRequestMessage, TResponseMessage, TResponse>(
        TRequestMessage requestMessage, CancellationToken cancellationToken)
        where TRequestMessage : struct, IStreamingRequestMessage<TRequestMessage, TResponseMessage>, allows ref struct
        where TResponseMessage : struct, IResponseMessage<TResponseMessage>, allows ref struct
        where TResponse : class, IResponse<TResponseMessage, TResponse>
    {
        var completionSource = this.cachedCompletionSources.Get<TResponse>();
        this.responseCompletionSource = completionSource;
        this.parser = ParseAndComplete<TResponseMessage, TResponse>;

        var primitiveWriter = new PrimitiveWriter(writer);
        primitiveWriter.Write(TRequestMessage.Type);
        requestMessage.Write(primitiveWriter);

        return StreamAndWait(requestMessage.Size, requestMessage.Body, completionSource, cancellationToken.Register(static c => ((ResponseCompletionSource<TResponse>)c!).Cancel(), completionSource));
    }


    private ValueTask Flush(CancellationToken cancellationToken)
    {
        var flushTask = writer.FlushAsync(cancellationToken);
        if (!flushTask.IsCompletedSuccessfully)
        {
            return FlushAsync(flushTask);
        }

        // Observe the result so a faulted flush is not swallowed.
        flushTask.GetAwaiter().GetResult();
        return default;

        [AsyncMethodBuilder(typeof(PoolingAsyncValueTaskMethodBuilder))]
        static async ValueTask FlushAsync(ValueTask<FlushResult> flushTask)
        {
            await flushTask.ConfigureAwait(false);
        }
    }

    /// <summary>
    /// Bodies are framed as length-prefixed chunks ending in a zero-length one, so a known and an
    /// unknown size produce the same shape on the wire and the reader needs only one code path.
    /// </summary>
    [AsyncMethodBuilder(typeof(PoolingAsyncValueTaskMethodBuilder))]
    private async ValueTask StreamMessage(int? size, PipeReader payload, CancellationToken cancellationToken)
    {
        if (size is { } known && (uint)known <= ProtocolConstants.MaxChunkSize)
        {
            if (known > 0)
            {
                WriteChunkHeader(known);
                await CopyExactly(payload, known, cancellationToken).ConfigureAwait(false);
            }
        }
        else
        {
            await CopyChunked(payload, cancellationToken).ConfigureAwait(false);
        }

        WriteChunkHeader(0);
        await writer.FlushAsync(cancellationToken).ConfigureAwait(false);
    }

    private async ValueTask CopyExactly(PipeReader payload, long count, CancellationToken cancellationToken)
    {
        while (count > 0)
        {
            var result = await payload.ReadAsync(cancellationToken).ConfigureAwait(false);
            var chunk = result.Buffer.Length <= count ? result.Buffer : result.Buffer.Slice(0, count);

            if (chunk.IsEmpty)
            {
                if (result.IsCompleted)
                {
                    ThrowShortBody(count);
                }

                payload.AdvanceTo(result.Buffer.Start, result.Buffer.End);
                continue;
            }

            WriteSequence(chunk);
            count -= chunk.Length;
            payload.AdvanceTo(chunk.End);
            await writer.FlushAsync(cancellationToken).ConfigureAwait(false);
        }
    }

    private async ValueTask CopyChunked(PipeReader payload, CancellationToken cancellationToken)
    {
        while (true)
        {
            var result = await payload.ReadAsync(cancellationToken).ConfigureAwait(false);
            var remaining = result.Buffer;

            // Clamped rather than cast: the chunk size is ours to choose, so an oversized read just
            // becomes several chunks and no length can overflow the header.
            while (!remaining.IsEmpty)
            {
                var chunk = remaining.Length <= ProtocolConstants.MaxChunkSize
                    ? remaining
                    : remaining.Slice(0, ProtocolConstants.MaxChunkSize);

                WriteChunkHeader((int)chunk.Length);
                WriteSequence(chunk);
                remaining = remaining.Slice(chunk.End);
            }

            payload.AdvanceTo(result.Buffer.End);
            await writer.FlushAsync(cancellationToken).ConfigureAwait(false);

            if (result.IsCompleted)
            {
                return;
            }
        }
    }

    private void WriteChunkHeader(int length)
    {
        var primitiveWriter = new PrimitiveWriter(writer);
        primitiveWriter.Write(length);
    }

    private void WriteSequence(ReadOnlySequence<byte> value)
    {
        foreach (var segment in value)
        {
            writer.Write(segment.Span);
        }
    }

    private async ValueTask<TResponse> StreamAndWait<TResponse>(int? size, PipeReader payload, ResponseCompletionSource<TResponse> responseCompletion, CancellationTokenRegistration cancellation) where TResponse : class
    {
        await StreamMessage(size, payload, cancellation.Token).ConfigureAwait(false);
        var result = await responseCompletion.ValueTask.ConfigureAwait(false);
        await cancellation.DisposeAsync();
        return result;
    }


    [AsyncMethodBuilder(typeof(PoolingAsyncValueTaskMethodBuilder<>))]
    private static async ValueTask<TResponse> SendAndWait<TResponse>(ValueTask<FlushResult> writerTask,
        ResponseCompletionSource<TResponse> responseCompletion, CancellationTokenRegistration cancellation) where TResponse : class
    {
        await writerTask.ConfigureAwait(false);
        var result = await responseCompletion.ValueTask.ConfigureAwait(false);
        await cancellation.DisposeAsync();
        return result;
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
    private static void ThrowShortBody(long missing) =>
        throw new InvalidOperationException($"Body ended with {missing} bytes still declared.");

    [DoesNotReturn]
    [StackTraceHidden]
    private static void ThrowUnsolicited(ResponseType responseType) =>
        throw new InvalidOperationException($"Received {responseType} with no request outstanding.");

    [DoesNotReturn]
    [StackTraceHidden]
    private static void ThrowWrongResponse(ResponseType received, ResponseType expected) =>
        throw new InvalidOperationException($"Received {received} while awaiting {expected}.");
}
