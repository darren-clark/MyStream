using System.Buffers;
using System.Diagnostics;
using System.Diagnostics.CodeAnalysis;
using System.IO.Pipelines;
using System.Runtime.CompilerServices;

using MyStream.Protocol.Messages;
using MyStream.Protocol.Primitives;
using MyStream.Protocol.Responses;

namespace MyStream.Protocol;


/// <summary>
/// The outbound half, and the half that does not depend on the handler: sending requests and
/// completing the responses to them. Both roles send, so this is deliberately not role-specific.
/// </summary>
public abstract class ConnectionBase(PipeReader reader, PipeWriter writer)
{
    /// <summary>A static method group, so the conversion is cached rather than allocated per send.</summary>
    private delegate ResponseParseResult ResponseParser(ConnectionBase connectionBase, ResponseType responseType,
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

    // Reusable wrapped pipe reader for streaming responses.
    // Used both on the write side(to write streaming response) and read side(to accept a streaming response).
    protected ChunkedPipeReader ResponseBodyReader { get; } = new(reader);

    public abstract Task Start(CancellationToken cancellationToken);
    
    /// <summary>
    ///  Ensures that only one message is in flight at a time.
    /// </summary>
    private void BeginSend()
    {
        if (Interlocked.Exchange(ref this.sendInFlight, 1) != 0)
        {
            ThrowConcurrentSend();
        }
    }

    private void EndSend() => Volatile.Write(ref this.sendInFlight, 0);

    /// <summary>
    /// Sends a message that does not require a response.
    ///
    /// Returns when pipe has been flushed.
    /// </summary>
    public ValueTask SendMessage<TRequestMessage>(TRequestMessage requestMessage, CancellationToken cancellationToken)
        where TRequestMessage : struct, IRequestMessage<TRequestMessage>, allows ref struct
    {
        BeginSend();
        try
        {
            var primitiveWriter = new PrimitiveWriter(writer);
            primitiveWriter.WriteEnum(TRequestMessage.Type);
            requestMessage.Write(primitiveWriter);
        }
        catch
        {
            EndSend();
            throw;
        }

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
        static async ValueTask FlushAsync(ConnectionBase connectionBase, ValueTask<FlushResult> flushTask)
        {
            try
            {
                await flushTask.ConfigureAwait(false);
            }
            finally
            {
                connectionBase.EndSend();
            }
        }
    }

    /// <summary>
    /// Sends a message with an expected response
    ///
    /// Waits for the response to land and returns it.
    /// </summary>
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
            this.parser = TryParseAndComplete<TResponseMessage, TResponse>;

            var primitiveWriter = new PrimitiveWriter(writer);
            primitiveWriter.WriteEnum(TRequestMessage.Type);
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
    
    public ValueTask<TResponseMessage> SendMessage<TRequestMessage, TResponseMessage>(
        TRequestMessage requestMessage, CancellationToken cancellationToken)
        where TRequestMessage : struct, IRequestMessage<TRequestMessage, TResponseMessage>, allows ref struct
        where TResponseMessage : struct, IResponseMessage<TResponseMessage>
    {
        BeginSend();
        ResponseCompletionSource<TResponseMessage> completionSource;
        try
        {
            completionSource = this.cachedCompletionSources.Get<TResponseMessage>();
            this.responseCompletionSource = completionSource;
            this.parser = TryParseAndComplete<TResponseMessage>;

            var primitiveWriter = new PrimitiveWriter(writer);
            primitiveWriter.WriteEnum(TRequestMessage.Type);
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
            cancellationToken.Register(static c => ((ResponseCompletionSource<TResponseMessage>)c!).Cancel(), completionSource));
    }
    
    [AsyncMethodBuilder(typeof(PoolingAsyncValueTaskMethodBuilder<>))]
    static async ValueTask<TResponse> SendAndWait<TResponse>(ConnectionBase connectionBase,
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
            connectionBase.EndSend();
        }
    }
    
    /// <summary>
    /// Sends a streaming message, with no response
    /// </summary>
    public ValueTask SendMessage<TRequestMessage>(StreamingMessage<TRequestMessage> message,
        CancellationToken cancellationToken)
        where TRequestMessage : struct, IStreamingRequestMessage<TRequestMessage>, allows ref struct
    {
        BeginSend();
        try
        {
            var primitiveWriter = new PrimitiveWriter(writer);
            primitiveWriter.WriteEnum(TRequestMessage.Type);
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
    /// Sends a streamed message with a response.
    /// </summary>
    public ValueTask<TResponse> SendMessage<TRequestMessage, TResponseMessage, TResponse>(
        StreamingMessage<TRequestMessage> message, CancellationToken cancellationToken)
        where TRequestMessage : struct, IStreamingRequestMessage<TRequestMessage, TResponseMessage>, allows ref struct
        where TResponseMessage : struct, IResponseMessage<TResponseMessage>, allows ref struct
        where TResponse : IResponse<TResponseMessage, TResponse>
    {
        BeginSend();
        ResponseCompletionSource<TResponse> completionSource;
        try
        {
            completionSource = this.cachedCompletionSources.Get<TResponse>();
            this.responseCompletionSource = completionSource;
            this.parser = TryParseAndComplete<TResponseMessage, TResponse>;

            var primitiveWriter = new PrimitiveWriter(writer);
            primitiveWriter.WriteEnum(TRequestMessage.Type);
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
    
    async ValueTask<TResponse> StreamAndWait<TResponse>(long? size, PipeReader payload, ResponseCompletionSource<TResponse> responseCompletion, CancellationTokenRegistration cancellation)
    {
        try
        {
            await StreamMessage(size, payload, cancellation.Token).ConfigureAwait(false);
            return await responseCompletion.ValueTask.ConfigureAwait(false);
        }
        finally
        {
            // In the finally so a canceled or faulted request does not leak the registration.
            await cancellation.DisposeAsync().ConfigureAwait(false);
            EndSend();
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

    /// <summary>
    /// Curried so the result type can be named while the message type is still inferred: C# cannot
    /// infer <typeparamref name="TResult"/> from the request's constraint, and type arguments are all or none.
    ///
    /// Only wraps AcknowledgeResponse{T} because otherwise you'd have to specify two type arguments anyway(ref struct and not).
    /// </summary>
    public readonly struct WithResultSender<TResult>(ConnectionBase connectionBase) where TResult : unmanaged, Enum
    {
        public ValueTask<TResult> Send<TRequestMessage>(TRequestMessage requestMessage,
            CancellationToken cancellationToken) where TRequestMessage : struct, IRequestMessage<TRequestMessage, AcknowledgeMessage<TResult>>, allows ref struct
        {
            var sendTask = connectionBase.SendMessage<TRequestMessage, AcknowledgeMessage<TResult>, AcknowledgeResponse<TResult>>(requestMessage,
                cancellationToken);

            return AwaitAndUnwrap(sendTask);
        }

        public ValueTask<TResult> Send<TRequestMessage>(StreamingMessage<TRequestMessage> message,
            CancellationToken cancellationToken)
            where TRequestMessage : struct, IStreamingRequestMessage<TRequestMessage, AcknowledgeMessage<TResult>>, allows ref struct
        {
            var sendTask =
                connectionBase.SendMessage<TRequestMessage, AcknowledgeMessage<TResult>, AcknowledgeResponse<TResult>>(
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


    /// <summary>
    /// Sends a message with a streaming response expected.
    /// </summary>
    public ValueTask<TResponse> SendMessageWithStreamedResponse<TRequestMessage, TResponseMessage, TResponse>(
        TRequestMessage requestMessage, CancellationToken cancellationToken)
        where TRequestMessage : struct, IRequestMessage<TRequestMessage, TResponseMessage>, allows ref struct
        where TResponseMessage : struct, IResponseMessage<TResponseMessage>, allows ref struct
        where TResponse : IStreamingResponse<TResponseMessage, TResponse>
    {
        BeginSend();
        ResponseCompletionSource<TResponse> completionSource;
        try
        {
            completionSource = this.cachedCompletionSources.Get<TResponse>();
            this.responseCompletionSource = completionSource;
            this.parser = ParseAndStream<TResponseMessage, TResponse>;

            var primitiveWriter = new PrimitiveWriter(writer);
            primitiveWriter.WriteEnum(TRequestMessage.Type);
            requestMessage.Write(primitiveWriter);
        }
        catch
        {
            EndSend();
            throw;
        }

        return SendAndWaitStreamed(
            this,
            writer.FlushAsync(cancellationToken),
            completionSource,
            cancellationToken.Register(static c => ((ResponseCompletionSource<TResponse>)c!).Cancel(), completionSource));
    }

    /// <summary>
    /// Releases the send on delivery like the buffered path: the body that follows is the awaiting
    /// caller's to drain, and the read loop will not look at the pipe again until it has.
    /// </summary>
    [AsyncMethodBuilder(typeof(PoolingAsyncValueTaskMethodBuilder<>))]
    private static async ValueTask<TResponse> SendAndWaitStreamed<TResponse>(ConnectionBase connection,
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
    /// Method group to be assigned to this.parser
    /// </summary>
    /// <param name="connectionBase"></param>
    /// <param name="responseType"></param>
    /// <param name="reader"></param>
    /// <typeparam name="TResponseMessage"></typeparam>
    /// <typeparam name="TResponse"></typeparam>
    /// <returns></returns>
    private static ResponseParseResult TryParseAndComplete<TResponseMessage, TResponse>(ConnectionBase connectionBase,
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
            return ResponseParseResult.Insufficient(bytesNeeded);
        }

        connectionBase.parser = null;
        connectionBase.responseCompletionSource!.TrySetResult(TResponse.CreateResponse(in response));
        return ResponseParseResult.Completed();
    }
    
    private static ResponseParseResult TryParseAndComplete<TResponseMessage>(ConnectionBase connectionBase,
        ResponseType responseType, ref PrimitiveReader reader)
        where TResponseMessage : struct, IResponseMessage<TResponseMessage>
    {
        if (responseType != TResponseMessage.Type)
        {
            ThrowWrongResponse(responseType, TResponseMessage.Type);
        }

        if (!TResponseMessage.TryRead(ref reader, out var response, out var bytesNeeded))
        {
            return ResponseParseResult.Insufficient(bytesNeeded);
        }

        connectionBase.parser = null;
        connectionBase.responseCompletionSource!.TrySetResult(response);
        return ResponseParseResult.Completed();
    }
    
    /// <summary>
    /// Method group to be assigned to this.parser for streamed responses.
    /// </summary>
    private static ResponseParseResult ParseAndStream<TResponseMessage, TResponse>(ConnectionBase connectionBase,
        ResponseType responseType, ref PrimitiveReader reader)
        where TResponseMessage : struct, IResponseMessage<TResponseMessage>, allows ref struct
        where TResponse : IStreamingResponse<TResponseMessage, TResponse>
    {
        if (responseType != TResponseMessage.Type)
        {
            ThrowWrongResponse(responseType, TResponseMessage.Type);
        }

        if (!TResponseMessage.TryRead(ref reader, out var header, out var bytesNeeded))
        {
            return ResponseParseResult.Insufficient(bytesNeeded);
        }

        connectionBase.ResponseBodyReader.Begin(declaredSize: null, reader.Remaining);
        var response = TResponse.CreateResponse(in header, connectionBase.ResponseBodyReader);

        connectionBase.parser = null;
        connectionBase.responseCompletionSource!.TrySetResult(response);
        return ResponseParseResult.Streaming(response.BodyDrained);
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

    private static void WriteDeclaredSize(PrimitiveWriter writer, long? size) =>
        writer.Write(size is { } known and >= 0 ? known : -1L);
    
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

  
    
    /// <summary>
    /// A response only ever answers a request this side sent, so the outstanding request picks the
    /// decoder and the type byte is checked against it rather than switched on.
    /// </summary>
    private protected ResponseParseResult TryParseAndComplete(ResponseType responseType, ref PrimitiveReader reader)
    {
        if (this.parser is null)
        {
            ThrowUnsolicited(responseType);
        }

        return this.parser(this, responseType, ref reader);
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

#if false
    /// <summary>
    /// A one-way message whose body the sender writes directly rather than copying from a pipe, for
    /// a producer that has the bytes in hand. Holds the send until <see cref="EndOneWayBody"/>, so
    /// the caller must pair them.
    /// </summary>
    public MessageBodyWriter BeginOneWayBody<TRequestMessage>(TRequestMessage message)
        where TRequestMessage : struct, IStreamingRequestMessage<TRequestMessage>, allows ref struct
    {
        BeginSend();
        try
        {
            var primitiveWriter = new PrimitiveWriter(writer);
            primitiveWriter.Write(TRequestMessage.Type);
            message.Write(primitiveWriter);

            // Always streamed: a sender that writes its own body does not know the length up front.
            WriteDeclaredSize(primitiveWriter, null);
            return new MessageBodyWriter(writer);
        }
        catch
        {
            EndSend();
            throw;
        }
    }

    /// <summary>
    /// Releases the body's pooled chunk buffer as well as the send, so the pair cannot be completed
    /// in a way that leaks it.
    /// </summary>
    public void EndOneWayBody(MessageBodyWriter body)
    {
        body.Complete();
        EndSend();
    }
#endif    
}
