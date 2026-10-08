namespace ToyStream.Protocol.Dispatch.Direct;

using System.Buffers;
using System.IO.Pipelines;
using System.Runtime.CompilerServices;
using System.Runtime.InteropServices;
using Messages;

/// <summary>
/// Server role: receives requests, sends responses. Its switch is exactly the request set, and each
/// arm calls a private non-virtual method - no handler interface, no registration, no type test.
/// </summary>
public sealed class DirectServerConnection : DirectConnection
{
    private readonly ITopicService topics;

    public DirectServerConnection(PipeReader reader, PipeWriter writer, ITopicService topics,
        CancellationToken cancellationToken)
        : base(reader, writer, cancellationToken)
    {
        this.topics = topics;
        this.StartReading();
    }

    protected override async Task Read()
    {
        await Task.Yield();
        var buffer = ReadOnlySequence<byte>.Empty;

        try
        {
            while (!this.CancellationToken.IsCancellationRequested)
            {
                if (buffer.Length < sizeof(MessageType))
                {
                    var result = await this.Reader.ReadAsync(this.CancellationToken).ConfigureAwait(false);
                    buffer = result.Buffer;
                }

                var messageType = (MessageType)MemoryMarshal.Read<byte>(buffer.Slice(0, 1).FirstSpan);
                var body = buffer.Slice(1);
                ValueTask pending;

                switch (messageType)
                {
                    case MessageType.CreateTopic:
                    {
                        var primitiveReader = new PrimitiveReader(body);
                        if (!TryDecode<CreateTopicMessage>(ref primitiveReader, out var createTopic))
                        {
                            buffer = await this.RefillAsync(buffer).ConfigureAwait(false);
                            continue;
                        }

                        body = primitiveReader.Remaining;
                        pending = this.HandleCreateTopic(in createTopic);
                        break;
                    }

                    default:
                        throw new InvalidOperationException($"Server received unexpected message type {messageType}.");
                }

                // Advance only once the handler has consumed the message, which is a view over this buffer.
                buffer = body;
                this.Reader.AdvanceTo(buffer.Start);

                if (!pending.IsCompleted)
                {
                    await pending.ConfigureAwait(false);
                }
            }
        }
        catch (OperationCanceledException)
        {
        }
    }

    // Thin protocol adapter: the domain work lives behind ITopicService so it is testable without pipes.
    private ValueTask HandleCreateTopic(ref readonly CreateTopicMessage message)
    {
        var createTopic = this.topics.CreateTopic(message.TopicName, message.PartitionCount);
        if (createTopic.IsCompletedSuccessfully)
        {
            createTopic.GetAwaiter().GetResult();
            return this.Write(new AcknowledgeMessage());
        }

        return AwaitThenAcknowledge(this, createTopic);

        // The response is a ref struct, so the slow path has to build its own.
        [AsyncMethodBuilder(typeof(PoolingAsyncValueTaskMethodBuilder))]
        static async ValueTask AwaitThenAcknowledge(DirectServerConnection self, ValueTask pending)
        {
            await pending.ConfigureAwait(false);
            await self.Write(new AcknowledgeMessage()).ConfigureAwait(false);
        }
    }
}
