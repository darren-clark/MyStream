namespace ToyStream.Protocol.Test;

using System.Buffers;
using System.IO.Pipelines;

public class HandshakeTests
{
    private static readonly TimeSpan Generous = TimeSpan.FromSeconds(5);

    [Test]
    public async Task AcceptsAWellFormedHandshake()
    {
        var pipe = new Pipe();
        await ProtocolHandshake.WriteAsync(pipe.Writer, ConnectionRole.Producer, default);

        var (status, role) = await ProtocolHandshake.ReadAsync(pipe.Reader, Generous, default);

        Assert.That(status, Is.EqualTo(HandshakeStatus.Accepted));
        Assert.That(role, Is.EqualTo(ConnectionRole.Producer));
    }

    /// <summary>
    /// The read loop starts on whatever is left, so consuming a byte too many swallows the first
    /// message of a client that pipelined it behind the handshake.
    /// </summary>
    [Test]
    public async Task LeavesWhatFollowsForTheReadLoop()
    {
        var pipe = new Pipe();
        await ProtocolHandshake.WriteAsync(pipe.Writer, ConnectionRole.Admin, default);
        await pipe.Writer.WriteAsync(new byte[] { 1, 2, 3 });

        await ProtocolHandshake.ReadAsync(pipe.Reader, Generous, default);

        var result = await pipe.Reader.ReadAsync();
        Assert.That(result.Buffer.ToArray(), Is.EqualTo(new byte[] { 1, 2, 3 }));
    }

    /// <summary>Without the magic this would surface as "unexpected message type 71" - the G of GET.</summary>
    [Test]
    public async Task RejectsAForeignProtocol()
    {
        var pipe = new Pipe();
        await pipe.Writer.WriteAsync("GET / HTTP/1.1"u8.ToArray());

        var (status, _) = await ProtocolHandshake.ReadAsync(pipe.Reader, Generous, default);

        Assert.That(status, Is.EqualTo(HandshakeStatus.NotToyStream));
    }

    [Test]
    public async Task RejectsAnUnsupportedVersion()
    {
        var pipe = new Pipe();
        await WriteHandshake(pipe.Writer, (byte)(ProtocolHandshake.CurrentVersion + 1), (byte)ConnectionRole.Admin);

        var (status, _) = await ProtocolHandshake.ReadAsync(pipe.Reader, Generous, default);

        Assert.That(status, Is.EqualTo(HandshakeStatus.UnsupportedVersion));
    }

    [Test]
    public async Task RejectsAnUnknownRole()
    {
        var pipe = new Pipe();
        await WriteHandshake(pipe.Writer, ProtocolHandshake.CurrentVersion, 99);

        var (status, _) = await ProtocolHandshake.ReadAsync(pipe.Reader, Generous, default);

        Assert.That(status, Is.EqualTo(HandshakeStatus.UnknownRole));
    }

    /// <summary>A peer that connects and goes quiet holds a socket while proving nothing.</summary>
    [Test]
    public async Task TimesOutAPeerThatNeverSendsOne()
    {
        var pipe = new Pipe();

        var (status, _) = await ProtocolHandshake.ReadAsync(pipe.Reader, TimeSpan.FromMilliseconds(100), default);

        Assert.That(status, Is.EqualTo(HandshakeStatus.Incomplete));
    }

    [Test]
    public async Task TreatsATruncatedHandshakeAsIncomplete()
    {
        var pipe = new Pipe();
        await pipe.Writer.WriteAsync(ProtocolHandshake.Magic.ToArray());
        await pipe.Writer.CompleteAsync();

        var (status, _) = await ProtocolHandshake.ReadAsync(pipe.Reader, Generous, default);

        Assert.That(status, Is.EqualTo(HandshakeStatus.Incomplete));
    }

    /// <summary>Closing silently is indistinguishable from a network fault when debugging.</summary>
    [Test]
    public async Task RejectionTellsThePeerWhy()
    {
        var pipe = new Pipe();

        await ProtocolHandshake.RejectAsync(pipe.Writer, HandshakeStatus.UnsupportedVersion, default);

        var result = await pipe.Reader.ReadAsync();
        Assert.That(result.Buffer.ToArray(),
            Is.EqualTo(new[] { (byte)HandshakeStatus.UnsupportedVersion }));
    }

    /// <summary>The response flag keeps a refusal from being read as a message.</summary>
    [Test]
    public void EveryRejectionCarriesTheResponseFlag()
    {
        foreach (var status in Enum.GetValues<HandshakeStatus>())
        {
            Assert.That(ProtocolConstants.IsResponse((byte)status), Is.True, $"{status} is routable as a message");
        }
    }

    private static ValueTask<FlushResult> WriteHandshake(PipeWriter writer, byte version, byte role)
    {
        byte[] bytes = [.. ProtocolHandshake.Magic, version, role];
        return writer.WriteAsync(bytes);
    }
}
