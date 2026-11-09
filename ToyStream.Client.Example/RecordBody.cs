namespace ToyStream.Client.Example;

using System.Buffers;
using System.IO.Pipelines;
using System.Text;

/// <summary>
/// Both cursors hand a body out as a stream rather than a buffer, so an example that only wants to
/// log it has to drain it somewhere first.
/// </summary>
internal static class RecordBody
{
    /// <summary>
    /// Only safe for bodies that fit the pipe's buffer, since nothing reads until the copy is done.
    /// </summary>
    public static async ValueTask<string> ReadAsString(Func<PipeWriter, CancellationToken, ValueTask> copyBodyTo,
        CancellationToken cancellationToken)
    {
        var pipe = new Pipe();
        await copyBodyTo(pipe.Writer, cancellationToken);
        await pipe.Writer.CompleteAsync();

        var read = await pipe.Reader.ReadAsync(cancellationToken);
        var body = Encoding.UTF8.GetString(read.Buffer);

        pipe.Reader.AdvanceTo(read.Buffer.End);
        await pipe.Reader.CompleteAsync();
        return body;
    }

    public static string AsText(this ReadOnlyMemory<byte> key) => Encoding.UTF8.GetString(key.Span);
}
