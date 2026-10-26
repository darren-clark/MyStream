using Microsoft.Extensions.DependencyInjection;

namespace ToyStream.Server;

using Protocol;

/// <summary>
/// Turns an accepted connection into the one role it declared. The role is read before any read loop
/// starts, so a connection's switch is fixed for its life rather than negotiated mid-stream.
/// </summary>
/// <remarks>
/// Takes a transport rather than deriving per transport: nothing here varies with how the peer
/// connected, so a socket, a loopback pipe and a test double are all just an
/// <see cref="ITransportConnection"/>.
/// </remarks>
public sealed class ConnectionAcceptor(IServiceProvider serviceProvider)
{
    /// <summary>
    /// Until the handshake lands a connection has cost a socket and a pending read while proving
    /// nothing, so a peer that connects and goes quiet must not park forever.
    /// </summary>
    private static readonly TimeSpan HandshakeTimeout = TimeSpan.FromSeconds(10);

    public async Task Accept(ITransportConnection transport, CancellationToken cancellationToken)
    {
        // The acceptor owns the transport for its whole life, whatever it is, so teardown does not
        // depend on which caller opened it.
        await using (transport.ConfigureAwait(false))
        {
            try
            {
                var (status, role) = await ProtocolHandshake
                    .ReadAsync(transport.Input, HandshakeTimeout, cancellationToken)
                    .ConfigureAwait(false);

                // Checked before the role is looked at, so nothing is built for a peer already refused.
                var connection = status == HandshakeStatus.Accepted
                    ? role switch
                    {
                        ConnectionRole.Admin => GetConnection<AdminHandler>(),
                        ConnectionRole.Producer => GetConnection<ProducerHandler>(),
                        ConnectionRole.Consumer => GetConnection<ConsumerHandler>(),

                        // Subscriptions arrive with the push work; the role is in the wire contract
                        // already so that adding it later is not a version bump.
                        _ => null
                    }
                    : null;

                if (connection is null)
                {
                    var refusal = status == HandshakeStatus.Accepted ? HandshakeStatus.UnknownRole : status;
                    await ProtocolHandshake.RejectAsync(transport.Output, refusal, cancellationToken)
                        .ConfigureAwait(false);
                    return;
                }

                await connection.Start(cancellationToken).ConfigureAwait(false);
            }
            catch (OperationCanceledException)
            {
            }
        }

        return;

        // Built from its service dependencies rather than resolved: the container never holds a
        // struct, so registration stays on the generic API and nothing is boxed per connection.
        ConnectionBase GetConnection<THandler>() where THandler : struct, IConnectionHandler =>
            new Connection<THandler>(transport.Input, transport.Output,
                ActivatorUtilities.CreateInstance<THandler>(serviceProvider));
    }
}
