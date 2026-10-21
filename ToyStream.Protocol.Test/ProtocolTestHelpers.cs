namespace ToyStream.Protocol.Test.Messages;

using System.Buffers;
using System.IO.Pipelines;

/// <summary>
/// PrimitiveWriter writes to a PipeWriter, so tests supply one backed by an ArrayBufferWriter rather
/// than standing up a real pipe just to inspect the bytes.
/// </summary>
internal sealed class TestPipeWriter : PipeWriter
{
    private readonly ArrayBufferWriter<byte> buffer = new(initialCapacity: 1024);

    public ReadOnlySpan<byte> WrittenSpan => this.buffer.WrittenSpan;

    public int WrittenCount => this.buffer.WrittenCount;

    public override void Advance(int bytes) => this.buffer.Advance(bytes);

    public override Memory<byte> GetMemory(int sizeHint = 0) => this.buffer.GetMemory(sizeHint);

    public override Span<byte> GetSpan(int sizeHint = 0) => this.buffer.GetSpan(sizeHint);

    public override void CancelPendingFlush()
    {
    }

    public override void Complete(Exception? exception = null)
    {
    }

    public override ValueTask<FlushResult> FlushAsync(CancellationToken cancellationToken = default) =>
        new(new FlushResult(isCanceled: false, isCompleted: false));
}

internal static class ProtocolTestHelpers
{
    public static bool TryReadMessage<TMessage>(ref PrimitiveReader reader, out TMessage message, out int bytesToRead)
        where TMessage : struct, IMessage<TMessage>, allows ref struct
        => TMessage.TryRead(ref reader, out message, out bytesToRead);

    // A static abstract member is reachable only through a type parameter, so the call is relayed.
    public static TResponse CreateStreamedResponse<TResponseMessage, TResponse>(
        ref readonly TResponseMessage header, PipeReader body)
        where TResponseMessage : struct, IResponseMessage<TResponseMessage>, allows ref struct
        where TResponse : IStreamingResponse<TResponseMessage, TResponse>
        => TResponse.CreateResponse(in header, body);

    // Messages are ref structs, so they can't be cast to IMessage<T> to reach the explicit implementation.
    public static void WriteMessage<TMessage>(TMessage message, PrimitiveWriter writer)
        where TMessage : struct, IMessage<TMessage>, allows ref struct
        => message.Write(writer);

    public static void GetWriteBuffer(out TestPipeWriter buffer, out PrimitiveWriter writer, int size = 1024)
    {
        buffer = new TestPipeWriter();
        writer = new PrimitiveWriter(buffer);
    }

    public static void GetReadBuffer(out Span<byte> buffer, out PrimitiveReader reader, int size = 1024)
    {
        var array = new byte[size];
        buffer = array;
        reader = new(new ReadOnlySequence<byte>(array));
    }
}
