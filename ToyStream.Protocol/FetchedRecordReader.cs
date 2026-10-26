using System.Buffers;
using System.Buffers.Binary;
using System.Diagnostics;
using System.Diagnostics.CodeAnalysis;
using System.IO.Pipelines;
using ToyStream.Protocol.Messages;
using ToyStream.Shared;

namespace ToyStream.Protocol;

/// <summary>
/// Walks the record stream written by <see cref="FetchedRecordWriter"/>, a record at a time. The
/// same bytes carry a fetch's answer and a subscription's push, so both read them through this.
/// </summary>
/// <remarks>Nothing but the record in hand is held, so no record has a maximum size.</remarks>
public sealed class FetchedRecordReader(PipeReader body)
{
    /// <summary>Doubles as the scratch for a body's chunk headers, which are read after it.</summary>
    private readonly byte[] prelude = new byte[FetchedRecordWriter.PreludeSize];

    private byte[] key = [];
    private int keyLength;
    private bool bodyPending;
    private bool finished;

    public FetchedRecordHeader Current { get; private set; }

    public ReadOnlyMemory<byte> Key => this.key.AsMemory(0, this.keyLength);

    /// <summary>
    /// Advances to the next record, skipping the body of the current one if it was not read. False
    /// once the stream is exhausted.
    /// </summary>
    public async ValueTask<bool> MoveNextAsync(CancellationToken cancellationToken = default)
    {
        if (this.finished)
        {
            return false;
        }

        if (this.bodyPending)
        {
            await DrainBody(destination: null, cancellationToken).ConfigureAwait(false);
        }

        if (!await TryReadExact(this.prelude.AsMemory(0, 1), cancellationToken).ConfigureAwait(false) ||
            this.prelude[0] == FetchedRecordWriter.EndOfRecords)
        {
            this.finished = true;
            return false;
        }

        if (this.prelude[0] != FetchedRecordWriter.RecordFollows)
        {
            ThrowInvalidMarker(this.prelude[0]);
        }

        await ReadExact(this.prelude, cancellationToken).ConfigureAwait(false);

        var fields = this.prelude.AsSpan();
        Current = new FetchedRecordHeader(
            BinaryPrimitives.ReadInt16BigEndian(fields),
            BinaryPrimitives.ReadInt16BigEndian(fields[2..]),
            BinaryPrimitives.ReadInt64BigEndian(fields[4..]),
            new DateTime(BinaryPrimitives.ReadInt64BigEndian(fields[12..]), DateTimeKind.Utc));

        var length = BinaryPrimitives.ReadInt32BigEndian(fields[20..]);
        if ((uint)length > ProtocolConstants.MaxKeySize)
        {
            ThrowHelpers.ThrowInvalidLength(length, ProtocolConstants.MaxKeySize);
        }

        if (this.key.Length < length)
        {
            this.key = new byte[length];
        }

        await ReadExact(this.key.AsMemory(0, length), cancellationToken).ConfigureAwait(false);
        this.keyLength = length;
        this.bodyPending = true;
        return true;
    }

    /// <summary>
    /// Streams the current record's body out. Flushed as it goes, so a record larger than the
    /// destination's buffer paces the sender rather than being held whole anywhere.
    /// </summary>
    public ValueTask CopyBodyTo(PipeWriter destination, CancellationToken cancellationToken = default)
    {
        ArgumentNullException.ThrowIfNull(destination);
        if (!this.bodyPending)
        {
            ThrowNoCurrentRecord();
        }

        return DrainBody(destination, cancellationToken);
    }

    private async ValueTask DrainBody(PipeWriter? destination, CancellationToken cancellationToken)
    {
        while (true)
        {
            await ReadExact(this.prelude.AsMemory(0, sizeof(int)), cancellationToken).ConfigureAwait(false);
            var chunk = BinaryPrimitives.ReadInt32BigEndian(this.prelude);
            if (chunk == 0)
            {
                break;
            }

            // Unsigned so a negative length is rejected by the same comparison as an oversized one.
            if ((uint)chunk > ProtocolConstants.MaxChunkSize)
            {
                ThrowHelpers.ThrowInvalidLength(chunk, ProtocolConstants.MaxChunkSize);
            }

            await Copy(destination, chunk, cancellationToken).ConfigureAwait(false);
        }

        this.bodyPending = false;
    }

    private async ValueTask Copy(PipeWriter? destination, int count, CancellationToken cancellationToken)
    {
        while (count > 0)
        {
            var result = await body.ReadAsync(cancellationToken).ConfigureAwait(false);
            if (result.Buffer.IsEmpty)
            {
                ThrowTruncated(count);
            }

            var taken = result.Buffer.Length <= count ? result.Buffer : result.Buffer.Slice(0, count);
            if (destination is not null)
            {
                foreach (var segment in taken)
                {
                    destination.Write(segment.Span);
                }
            }

            body.AdvanceTo(taken.End);
            count -= (int)taken.Length;

            if (destination is not null)
            {
                await destination.FlushAsync(cancellationToken).ConfigureAwait(false);
            }
        }
    }

    /// <summary>
    /// The body arrives a chunk at a time, so a field straddling two of them cannot be read as one
    /// view and is copied out instead. Small and fixed width, so the copy is not worth avoiding.
    /// </summary>
    private async ValueTask<bool> TryReadExact(Memory<byte> destination, CancellationToken cancellationToken)
    {
        var written = 0;
        while (written < destination.Length)
        {
            var result = await body.ReadAsync(cancellationToken).ConfigureAwait(false);
            if (result.Buffer.IsEmpty)
            {
                if (written > 0)
                {
                    ThrowTruncated(destination.Length - written);
                }

                return false;
            }

            var taken = result.Buffer.Length <= destination.Length - written
                ? result.Buffer
                : result.Buffer.Slice(0, destination.Length - written);

            taken.CopyTo(destination.Span[written..]);
            body.AdvanceTo(taken.End);
            written += (int)taken.Length;
        }

        return true;
    }

    private async ValueTask ReadExact(Memory<byte> destination, CancellationToken cancellationToken)
    {
        if (destination.Length > 0 &&
            !await TryReadExact(destination, cancellationToken).ConfigureAwait(false))
        {
            ThrowTruncated(destination.Length);
        }
    }

    [DoesNotReturn]
    [StackTraceHidden]
    private static void ThrowInvalidMarker(byte marker) =>
        throw new InvalidOperationException($"Expected a record marker but read {marker}.");

    [DoesNotReturn]
    [StackTraceHidden]
    private static void ThrowTruncated(long missing) =>
        throw new InvalidOperationException($"Record stream ended with {missing} bytes outstanding.");

    [DoesNotReturn]
    [StackTraceHidden]
    private static void ThrowNoCurrentRecord() =>
        throw new InvalidOperationException("There is no record to read; call MoveNextAsync first.");
}
