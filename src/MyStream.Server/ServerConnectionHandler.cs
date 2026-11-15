using Microsoft.Extensions.DependencyInjection;
using Microsoft.Extensions.Options;

using MyStream.Protocol;
using MyStream.Server.Roles.Admin;

using ToyStream.Protocol;

namespace MyStream.Server;

public class ServerConnectionHandler(IOptions<ServerOptions> options, IServiceProvider serviceProvider)
{
    public async Task Start(ITransportConnection connection, CancellationToken cancellationToken)
    {
        using var timed = CancellationTokenSource.CreateLinkedTokenSource(cancellationToken);
        timed.CancelAfter(options.Value.HandshakeTimeout);
        await using (connection.ConfigureAwait(false))
        {
            try
            {
                var (status, role) = await ProtocolHandshake
                    .ReadAsync(connection.Input, timed.Token)
                    .ConfigureAwait(false);

                // Checked before the role is looked at, so nothing is built for a peer already refused.
                (status, var serverConnection) = status == HandshakeStatus.Accepted
                    ? role switch
                    {
                        ConnectionRole.Admin => await InitConnection<AdminHandler>(timed.Token).ConfigureAwait(false),
                        // ConnectionRole.Producer => GetConnection<ProducerHandler>(),
                        // ConnectionRole.Consumer => GetConnection<ConsumerHandler>(),
                     //   ConnectionRole.Subscriber => null,
                        _ => (HandshakeStatus.UnknownRole, null)
                    }
                    : (status, null);

                // if (role == ConnectionRole.Subscriber && status == HandshakeStatus.Accepted)
                // {
                //     await Subscribe(transport, cancellationToken).ConfigureAwait(false);
                //     return;
                // }

                if (status != HandshakeStatus.Accepted)
                {
                    await ProtocolHandshake.RejectAsync(connection.Output, status, cancellationToken)
                        .ConfigureAwait(false);
                    return;
                }

                await serverConnection!.Start(cancellationToken).ConfigureAwait(false);
            }
            catch (OperationCanceledException)
            {
            }

            return;

            async Task<(HandshakeStatus, ConnectionBase?)> InitConnection<THandler>(CancellationToken timeoutToken) where THandler : struct, IConnectionHandler
            {
                var handler = ActivatorUtilities.CreateInstance<THandler>(serviceProvider);
                var status = await handler.Initialize(connection.Input, timeoutToken).ConfigureAwait(false);

                return (status,
                    status == HandshakeStatus.Accepted
                        ? new Connection<THandler>(connection.Input, connection.Output, handler)
                        : null);
            }
        }
    }
}