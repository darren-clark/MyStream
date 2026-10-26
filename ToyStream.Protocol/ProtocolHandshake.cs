using System.Buffers;
using System.IO.Pipelines;

namespace ToyStream.Protocol;

/// <summary>
/// Which half of the protocol a connection speaks. Fixed for the connection's life, so the role's
/// switch is exactly its message set and a misdirected message is a protocol violation.
/// </summary>
public enum ConnectionRole : byte
{
    Admin = 1,
    Producer = 2,
    Consumer = 3,
    Subscriber = 4
}

/// <summary>
/// Sent only when a handshake is refused, so the happy path costs no round trip. The response flag
/// is set so a rejection can never be mistaken for a message.
/// </summary>
public enum HandshakeStatus : byte
{
    Accepted = ProtocolConstants.ResponseFlag,
    NotToyStream = ProtocolConstants.ResponseFlag | 1,
    UnsupportedVersion = ProtocolConstants.ResponseFlag | 2,
    UnknownRole = ProtocolConstants.ResponseFlag | 3,

    /// <summary>The peer never sent a full handshake - it closed, or it stopped talking.</summary>
    Incomplete = ProtocolConstants.ResponseFlag | 4
}

/// <summary>
/// Read before any read loop starts, so the role is known rather than negotiated mid-stream. Version
/// precedes role deliberately: everything after the version is the version's to define, so only the
/// magic is frozen for good.
/// </summary>
public static class ProtocolHandshake
{
    /// <summary>
    /// Its whole job is to detect a peer that is not speaking this protocol, which is why it is the
    /// one part that can never change.
    /// </summary>
    public static ReadOnlySpan<byte> Magic => "MYST"u8;

    public const byte CurrentVersion = 1;

    private const int Length = 4 + 1 + 1;

    public static ValueTask<FlushResult> WriteAsync(PipeWriter writer, ConnectionRole role,
        CancellationToken cancellationToken)
    {
        var span = writer.GetSpan(Length);
        Magic.CopyTo(span);
        span[Magic.Length] = CurrentVersion;
        span[Magic.Length + 1] = (byte)role;
        writer.Advance(Length);

        return writer.FlushAsync(cancellationToken);
    }

    /// <summary>
    /// Bounded by its own timeout: until this returns, a connection has cost a socket and a pending
    /// read while proving nothing, so a peer that connects and goes quiet must not park forever.
    /// </summary>
    public static async ValueTask<(HandshakeStatus Status, ConnectionRole Role)> ReadAsync(PipeReader reader,
        TimeSpan timeout, CancellationToken cancellationToken)
    {
        using var timed = CancellationTokenSource.CreateLinkedTokenSource(cancellationToken);
        timed.CancelAfter(timeout);

        ReadResult result;
        try
        {
            result = await reader.ReadAtLeastAsync(Length, timed.Token).ConfigureAwait(false);
        }
        catch (OperationCanceledException) when (!cancellationToken.IsCancellationRequested)
        {
            return (HandshakeStatus.Incomplete, default);
        }

        var buffer = result.Buffer;
        if (buffer.Length < Length)
        {
            reader.AdvanceTo(buffer.Start, buffer.End);
            return (HandshakeStatus.Incomplete, default);
        }

        // Copied out before advancing: the span is a view over segments the pipe is about to recycle.
        Span<byte> handshake = stackalloc byte[Length];
        buffer.Slice(0, Length).CopyTo(handshake);
        reader.AdvanceTo(buffer.GetPosition(Length));

        if (!handshake[..Magic.Length].SequenceEqual(Magic))
        {
            return (HandshakeStatus.NotToyStream, default);
        }

        if (handshake[Magic.Length] != CurrentVersion)
        {
            return (HandshakeStatus.UnsupportedVersion, default);
        }

        var role = (ConnectionRole)handshake[Magic.Length + 1];
        return Enum.IsDefined(role) ? (HandshakeStatus.Accepted, role) : (HandshakeStatus.UnknownRole, default);
    }

    /// <summary>Tells the peer why rather than closing silently, which reads as a network fault.</summary>
    public static async ValueTask RejectAsync(PipeWriter writer, HandshakeStatus status,
        CancellationToken cancellationToken)
    {
        writer.GetSpan(1)[0] = (byte)status;
        writer.Advance(1);

        await writer.FlushAsync(cancellationToken).ConfigureAwait(false);
        await writer.CompleteAsync().ConfigureAwait(false);
    }
}
