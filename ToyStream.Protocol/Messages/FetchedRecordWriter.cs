using System.Buffers;
using System.IO.Pipelines;
using ToyStream.Shared;

namespace ToyStream.Protocol.Messages;

/// <summary>
/// Writes the record stream that follows a fetch response header. A marker in front of each record
/// and a zero to finish, so the producer can stop wherever it likes; each body is chunk framed in
/// turn, so no record has to declare a size the sender does not yet know.
/// </summary>
public readonly struct FetchedRecordWriter(MessageBodyWriter body)
{
    internal const byte RecordFollows = 1;
    internal const byte EndOfRecords = 0;

    /// <summary>The fields in front of a record's body, all fixed width but for the key.</summary>
    internal const int PreludeSize = sizeof(short) + sizeof(short) + sizeof(long) + sizeof(long) + sizeof(int);

    /// <summary>
    /// How much is allowed to build up before a flush. Small enough that a reader starts work well
    /// before a backlog has finished writing, and that an unread pipe stops the writer rather than
    /// growing - which is what flushing was for.
    /// </summary>
    private const int FlushThreshold = 4 * 1024;

    /// <summary>
    /// Paced by how much has built up rather than flushed per record. A flush the reader has to wake
    /// for costs it a suspended state machine at every frame of the parse, and per record that was
    /// most of what delivering one allocated.
    /// </summary>
    public ValueTask<FlushResult> WriteRecord(in FetchedRecord record, CancellationToken cancellationToken)
    {
        var writer = new PrimitiveWriter(body);
        writer.Write(RecordFollows);
        writer.Write(record.TopicIndex);
        writer.Write(record.PartitionId);
        writer.Write(record.Offset);
        writer.Write(record.Timestamp.Ticks);
        writer.Write(new RefBytes(record.Key), ProtocolConstants.MaxKeySize);
        WriteBody(writer, record.Body);

        // The body is always flushed when it completes, so holding back cannot strand a record.
        return body.UnflushedBytes >= FlushThreshold ? body.FlushAsync(cancellationToken) : default;
    }

    public ValueTask<FlushResult> CompleteAsync(CancellationToken cancellationToken)
    {
        new PrimitiveWriter(body).Write(EndOfRecords);
        return body.CompleteBodyAsync(cancellationToken);
    }

    /// <summary>
    /// Chunked rather than length prefixed, so the only bound is on one chunk header and a record
    /// as a whole has no maximum size.
    /// </summary>
    private void WriteBody(PrimitiveWriter writer, ReadOnlySequence<byte> value)
    {
        var remaining = value;
        while (!remaining.IsEmpty)
        {
            var chunk = remaining.Length <= ProtocolConstants.MaxChunkSize
                ? remaining
                : remaining.Slice(0, ProtocolConstants.MaxChunkSize);

            writer.Write((int)chunk.Length);
            foreach (var segment in chunk)
            {
                body.Write(segment.Span);
            }

            remaining = remaining.Slice(chunk.End);
        }

        writer.Write(0);
    }
}
