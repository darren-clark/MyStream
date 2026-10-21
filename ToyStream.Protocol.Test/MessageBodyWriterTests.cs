namespace ToyStream.Protocol.Test;

using System.Buffers;
using System.IO.Pipelines;

/// <summary>
/// The writer exists to feed <see cref="MessageBodyReader"/>, so everything here goes out through
/// one and back in through the other rather than asserting on the framing directly.
/// </summary>
public class MessageBodyWriterTests
{
    [TestCase(1)]
    [TestCase(64)]
    [TestCase(8 * 1024)]
    [TestCase(256 * 1024)]
    public async Task BodyRoundTrips(int size)
    {
        var body = RandomBytes(size);

        Assert.That(await RoundTrip(body, chunkSize: 4096), Is.EqualTo(body));
    }

    /// <summary>A body far larger than one chunk must be split and reassembled.</summary>
    [Test]
    public async Task BodyLargerThanTheChunkIsSplit()
    {
        var body = RandomBytes((4096 * 5) + 17);

        Assert.That(await RoundTrip(body, chunkSize: 4096), Is.EqualTo(body));
    }

    [Test]
    public async Task EmptyBodyRoundTrips()
    {
        Assert.That(await RoundTrip([], chunkSize: 4096), Is.Empty);
    }

    /// <summary>A hint bigger than the chunk is honoured, so a caller can demand one contiguous span.</summary>
    [Test]
    public async Task OversizedSpanRequestIsHonoured()
    {
        var pipe = new Pipe();
        var reading = ReadBody(pipe.Reader);
        var writer = new MessageBodyWriter(pipe.Writer, chunkSize: 1024);

        var span = writer.GetSpan(9000);
        Assert.That(span.Length, Is.GreaterThanOrEqualTo(9000));

        var body = RandomBytes(9000);
        body.CopyTo(span);
        writer.Advance(body.Length);

        await writer.CompleteBodyAsync();
        writer.Complete();
        await pipe.Writer.CompleteAsync();

        Assert.That(await reading, Is.EqualTo(body));
    }

    /// <summary>Flushing mid body is what gives a slow reader backpressure, and must not corrupt it.</summary>
    [Test]
    public async Task FlushingBetweenWritesPreservesTheBody()
    {
        var pipe = new Pipe();
        var reading = ReadBody(pipe.Reader);
        var writer = new MessageBodyWriter(pipe.Writer, chunkSize: 4096);
        var expected = new List<byte>();

        for (var i = 0; i < 20; i++)
        {
            var part = RandomBytes(500);
            writer.Write(part);
            expected.AddRange(part);
            await writer.FlushAsync();
        }

        await writer.CompleteBodyAsync();
        writer.Complete();
        await pipe.Writer.CompleteAsync();

        Assert.That(await reading, Is.EqualTo(expected.ToArray()));
    }

    private static async Task<byte[]> RoundTrip(byte[] body, int chunkSize)
    {
        var pipe = new Pipe();

        // Drained concurrently: a body past the pipe's threshold blocks the writer until it is read,
        // which is the backpressure the writer exists to provide.
        var reading = ReadBody(pipe.Reader);

        var writer = new MessageBodyWriter(pipe.Writer, chunkSize);
        writer.Write(body);
        await writer.CompleteBodyAsync();
        writer.Complete();
        await pipe.Writer.CompleteAsync();

        return await reading;
    }

    private static async Task<byte[]> ReadBody(PipeReader source)
    {
        var reader = new MessageBodyReader(source);

        // Primed the way the connection primes it: Begin takes a buffer a read already produced.
        var first = await source.ReadAsync();
        reader.Begin(declaredSize: null, first.Buffer);

        var received = new ArrayBufferWriter<byte>();
        while (true)
        {
            var result = await reader.ReadAsync();
            if (result.Buffer.IsEmpty)
            {
                break;
            }

            foreach (var segment in result.Buffer)
            {
                received.Write(segment.Span);
            }

            reader.AdvanceTo(result.Buffer.End);
        }

        // The body reader leaves the inner pipe alone, so the connection is what releases it. A
        // reader that skips this starves the writer once the body passes the pipe's threshold.
        var remaining = await reader.FinishAsync(CancellationToken.None);
        source.AdvanceTo(remaining.Start, remaining.End);

        return received.WrittenSpan.ToArray();
    }

    private static byte[] RandomBytes(int count)
    {
        var bytes = new byte[count];
        Random.Shared.NextBytes(bytes);
        return bytes;
    }
}
