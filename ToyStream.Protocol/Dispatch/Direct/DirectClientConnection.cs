namespace ToyStream.Protocol.Dispatch.Direct;

using System.Buffers;
using System.IO.Pipelines;
using System.Runtime.InteropServices;
using Messages;
using Responses;
using Shared;

/// <summary>
/// Client role: sends requests, receives responses. Its switch is exactly the response set - it has
/// no CreateTopic arm at all, because a client receiving one is a protocol violation rather than an
/// unimplemented handler.
/// </summary>
public sealed class DirectClientConnection : DirectConnection
{
    public DirectClientConnection(PipeReader reader, PipeWriter writer, CancellationToken cancellationToken)
        : base(reader, writer, cancellationToken)
        => this.StartReading();

    public ValueTask<AcknowledgeResponse> CreateTopic(RefUtf8String topicName, short partitionCount)
        => this.SendMessage<CreateTopicMessage, AcknowledgeMessage, AcknowledgeResponse>(
            new CreateTopicMessage { TopicName = topicName, PartitionCount = partitionCount },
            default);

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

                switch (messageType)
                {
                    case MessageType.Acknowledge:
                    {
                        var primitiveReader = new PrimitiveReader(body);
                        if (!TryDecode<AcknowledgeMessage>(ref primitiveReader, out var acknowledge))
                        {
                            buffer = await this.RefillAsync(buffer).ConfigureAwait(false);
                            continue;
                        }

                        body = primitiveReader.Remaining;
                        this.CompleteResponse<AcknowledgeMessage, AcknowledgeResponse>(in acknowledge);
                        break;
                    }

                    default:
                        throw new InvalidOperationException($"Client received unexpected message type {messageType}.");
                }

                buffer = body;
                this.Reader.AdvanceTo(buffer.Start);
            }
        }
        catch (OperationCanceledException)
        {
        }
    }
}
