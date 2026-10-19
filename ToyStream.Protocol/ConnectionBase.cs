using ToyStream.Protocol.Responses;

namespace ToyStream.Protocol;

using System.Buffers;
using System.Diagnostics;
using System.Diagnostics.CodeAnalysis;
using System.IO.Pipelines;
using System.Runtime.CompilerServices;
using ToyStream.Protocol.Messages;

/// <summary>
/// The outbound half, and the half that does not depend on the handler: sending requests and
/// completing the responses to them. Both roles send, so this is deliberately not role-specific.
/// </summary>
public abstract class Connection(PipeWriter writer)
{
    public static Connection Create<THandler>(PipeReader reader, PipeWriter writer, THandler handler)
        where THandler : struct, IConnectionHandler
    {
        return new Connection<THandler>(reader, writer, handler);
    }

    /// <summary>A static method group, so the conversion is cached rather than allocated per send.</summary>
    private delegate DispatchResult ResponseParser(Connection connection, ResponseType responseType,
        ref PrimitiveReader reader);

    private readonly ResponseCompletionSourceCache cachedCompletionSources = new();

    private ResponseCompletionSource? responseCompletionSource;
    private ResponseParser? parser;

    /// <summary>
    /// Both the writer and the response slot are single-occupancy, so an overlapping send would
    /// interleave bytes on the wire and overwrite the pending completion. Interlocked rather than a
    /// plain field because the whole point is to catch a caller on another thread.
    /// </summary>
    private int sendInFlight;

    protected PipeWriter Writer => writer;

    private void BeginSend()
    {
        if (Interlocked.Exchange(ref this.sendInFlight, 1) != 0)
        {
            ThrowConcurrentSend();
        }
    }

    private void EndSend() => Volatile.Write(ref this.sendInFlight, 0);

    public ValueTask SendMessage<TRequestMessage>(TRequestMessage requestMessage, CancellationToken cancellationToken)
        where TRequestMessage : struct, IRequestMessage<TRequestMessage>, allows ref struct
    {
        BeginSend();
        try
        {
            var primitiveWriter = new PrimitiveWriter(writer);
            primitiveWriter.Write(TRequestMessage.Type);
            requestMessage.Write(primitiveWriter);
        }
        catch
        {
            EndSend();
            throw;
        }

        return Flush(cancellationToken);
    }
    
    public ValueTask SendStreamingMessage<TRequestMessage>(StreamingMessage<TRequestMessage> message,
        CancellationToken cancellationToken)
        where TRequestMessage : struct, IStreamingRequestMessage<TRequestMessage>, allows ref struct
    {
        BeginSend();
        try
        {
            var primitiveWriter = new PrimitiveWriter(writer);
            primitiveWriter.Write(TRequestMessage.Type);
            message.Header.Write(primitiveWriter);
            WriteDeclaredSize(primitiveWriter, message.Size);
        }
        catch
        {
            EndSend();
            throw;
        }

        return StreamAndRelease(message.Size, message.Body, cancellationToken);
    }

    [AsyncMethodBuilder(typeof(PoolingAsyncValueTaskMethodBuilder))]
    private async ValueTask StreamAndRelease(long? size, PipeReader payload, CancellationToken cancellationToken)
    {
        try
        {
            await StreamMessage(size, payload, cancellationToken).ConfigureAwait(false);
        }
        finally
        {
            EndSend();
        }
    }

    /// <summary>
    /// Curried so the result type can be named while the message type is still inferred: C# cannot
    /// infer <typeparamref name="TResult"/> from the request's constraint, and type arguments are all or none.
    /// </summary>
    public readonly struct WithResultSender<TResult>(Connection connection) where TResult : unmanaged, Enum
    {
        public ValueTask<TResult> Send<TRequestMessage>(TRequestMessage requestMessage,
            CancellationToken cancellationToken) where TRequestMessage : struct, IRequestMessage<TRequestMessage, AcknowledgeMessage<TResult>>, allows ref struct
        {
            var sendTask = connection.SendMessage<TRequestMessage, AcknowledgeMessage<TResult>, AcknowledgeResponse<TResult>>(requestMessage,
                cancellationToken);

            return AwaitAndUnwrap(sendTask);
        }

        public ValueTask<TResult> SendStreaming<TRequestMessage>(StreamingMessage<TRequestMessage> message,
            CancellationToken cancellationToken)
            where TRequestMessage : struct, IStreamingRequestMessage<TRequestMessage, AcknowledgeMessage<TResult>>, allows ref struct
        {
            var sendTask =
                connection.SendStreamingMessage<TRequestMessage, AcknowledgeMessage<TResult>, AcknowledgeResponse<TResult>>(
                    message, cancellationToken);

            return AwaitAndUnwrap(sendTask);
        }

        [AsyncMethodBuilder(typeof(PoolingAsyncValueTaskMethodBuilder<>))]
        private static async ValueTask<TResult> AwaitAndUnwrap(ValueTask<AcknowledgeResponse<TResult>> sendTask)
        {
            var response = await sendTask.ConfigureAwait(false);
            return response.Result;
        }
    }

    public WithResultSender<TResult> WithResult<TResult>() where TResult : unmanaged, Enum => new(this);

    public ValueTask<TResponse> SendMessage<TRequestMessage, TResponseMessage, TResponse>(
        TRequestMessage requestMessage, CancellationToken cancellationToken)
        where TRequestMessage : struct, IRequestMessage<TRequestMessage, TResponseMessage>, allows ref struct
        where TResponseMessage : struct, IResponseMessage<TResponseMessage>, allows ref struct
        where TResponse : IResponse<TResponseMessage, TResponse>
    {
        BeginSend();
        ResponseCompletionSource<TResponse> completionSource;
        try
        {
            completionSource = this.cachedCompletionSources.Get<TResponse>();
            this.responseCompletionSource = completionSource;
            this.parser = ParseAndComplete<TResponseMessage, TResponse>;

            var primitiveWriter = new PrimitiveWriter(writer);
            primitiveWriter.Write(TRequestMessage.Type);
            requestMessage.Write(primitiveWriter);
        }
        catch
        {
            EndSend();
            throw;
        }

        return SendAndWait(
            this,
            writer.FlushAsync(cancellationToken),
            completionSource,
            cancellationToken.Register(static c => ((ResponseCompletionSource<TResponse>)c!).Cancel(), completionSource));
    }

    /// <summary>
    /// Header then payload, so a large body is never buffered whole. The header write stays here
    /// because a ref struct cannot be a parameter of an async method; only the copy and the wait are.
    /// </summary>
    public ValueTask<TResponse> SendStreamingMessage<TRequestMessage, TResponseMessage, TResponse>(
        StreamingMessage<TRequestMessage> message, CancellationToken cancellationToken)
        where TRequestMessage : struct, IStreamingRequestMessage<TRequestMessage, TResponseMessage>, allows ref struct
        where TResponseMessage : struct, IResponseMessage<TResponseMessage>, allows ref struct
        where TResponse : class, IResponse<TResponseMessage, TResponse>
    {
        BeginSend();
        ResponseCompletionSource<TResponse> completionSource;
        try
        {
            completionSource = this.cachedCompletionSources.Get<TResponse>();
            this.responseCompletionSource = completionSource;
            this.parser = ParseAndComplete<TResponseMessage, TResponse>;

            var primitiveWriter = new PrimitiveWriter(writer);
            primitiveWriter.Write(TRequestMessage.Type);
            message.Header.Write(primitiveWriter);
            WriteDeclaredSize(primitiveWriter, message.Size);
        }
        catch
        {
            EndSend();
            throw;
        }

        return StreamAndWait(message.Size, message.Body, completionSource, cancellationToken.Register(static c => ((ResponseCompletionSource<TResponse>)c!).Cancel(), completionSource));
    }

    /// <summary>Framed here rather than by each message, so no streaming message can omit it.</summary>
    private static void WriteDeclaredSize(PrimitiveWriter writer, long? size) =>
        writer.Write(size is { } known and >= 0 ? known : -1L);


    private ValueTask Flush(CancellationToken cancellationToken)
    {
        var flushTask = writer.FlushAsync(cancellationToken);
        if (!flushTask.IsCompletedSuccessfully)
        {
            return FlushAsync(this, flushTask);
        }

        try
        {
            // Observe the result so a faulted flush is not swallowed.
            flushTask.GetAwaiter().GetResult();
        }
        finally
        {
            EndSend();
        }

        return default;

        [AsyncMethodBuilder(typeof(PoolingAsyncValueTaskMethodBuilder))]
        static async ValueTask FlushAsync(Connection connection, ValueTask<FlushResult> flushTask)
        {
            try
            {
                await flushTask.ConfigureAwait(false);
            }
            finally
            {
                connection.EndSend();
            }
        }
    }

    /// <summary>
    /// Bodies are framed as length-prefixed chunks ending in a zero-length one, so a known and an
    /// unknown size produce the same shape on the wire and the reader needs only one code path.
    /// </summary>
    [AsyncMethodBuilder(typeof(PoolingAsyncValueTaskMethodBuilder))]
    private async ValueTask StreamMessage(long? size, PipeReader payload, CancellationToken cancellationToken)
    {
        if (size is { } known && (ulong)known <= ProtocolConstants.MaxChunkSize)
        {
            if (known > 0)
            {
                WriteChunkHeader((int)known);
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

    private async ValueTask<TResponse> StreamAndWait<TResponse>(long? size, PipeReader payload, ResponseCompletionSource<TResponse> responseCompletion, CancellationTokenRegistration cancellation) where TResponse : class
    {
        try
        {
            await StreamMessage(size, payload, cancellation.Token).ConfigureAwait(false);
            return await responseCompletion.ValueTask.ConfigureAwait(false);
        }
        finally
        {
            // In the finally so a cancelled or faulted request does not leak the registration.
            await cancellation.DisposeAsync().ConfigureAwait(false);
            EndSend();
        }
    }


    [AsyncMethodBuilder(typeof(PoolingAsyncValueTaskMethodBuilder<>))]
    private static async ValueTask<TResponse> SendAndWait<TResponse>(Connection connection,
        ValueTask<FlushResult> writerTask,
        ResponseCompletionSource<TResponse> responseCompletion, CancellationTokenRegistration cancellation)
    {
        try
        {
            await writerTask.ConfigureAwait(false);
            return await responseCompletion.ValueTask.ConfigureAwait(false);
        }
        finally
        {
            await cancellation.DisposeAsync().ConfigureAwait(false);
            connection.EndSend();
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
        where TResponse : IResponse<TResponseMessage, TResponse>
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
    private static void ThrowConcurrentSend() =>
        throw new InvalidOperationException(
            "A send is already in flight. Connection supports one outstanding request and is not safe for concurrent use.");

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
