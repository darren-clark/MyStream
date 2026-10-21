using System.Diagnostics;
using System.IO.Pipelines;
using Microsoft.Extensions.ObjectPool;
using Microsoft.IO;
using ToyStream.Protocol.Responses;
using ToyStream.Shared;

namespace ToyStream.Server.InMemory;

/// <summary>
/// An in-memory partition has to materialise what it stores, so it copies each chunk as it arrives
/// rather than retaining the pipe memory it is handed.
/// </summary>
public sealed class InMemoryPartition : ITopicPartition
{
    private static readonly ObjectPool<EventWriter> writerPool =
        new DefaultObjectPool<EventWriter>(new EventWriterPoolPolicy());

    private static readonly RecyclableMemoryStreamManager streamManager = new();
    /// <summary>The declared size is the sender's claim, so it sizes the buffer only up to a bound.</summary>
    private const int MaxPreallocation = 1024 * 1024;

    private readonly EventLog log = new();

    private sealed class EventWriter : IEventWriter
    {
        private InMemoryPartition? partition;
        private Memory<byte>? key;
        private RecyclableMemoryStream? body;
        
        public void Initialize(InMemoryPartition partition, RefBytes key, long? size)
        {
            this.partition = partition;
            this.key = key.ToArray();

            // The stream reserves whatever it is told, and the size is the sender's claim.
            var capacity = size is > 0 and <= MaxPreallocation ? size.Value : 0;
            this.body = capacity > 0
                ? streamManager.GetStream(tag: null, requiredSize: capacity)
                : streamManager.GetStream();
        }

        public bool Release()
        {
            partition = null;
            key = null;
            body = null;
            return true;
        }

        public async ValueTask<PublishResult> WriteEvent(PipeReader input, CancellationToken cancellationToken)
        {
            // Released on the way out, so a second call would be writing into another publish's state.
            ObjectDisposedException.ThrowIf(this.partition is null, this);

            // Held locally because returning to the pool clears the field, and the committed event
            // keeps the stream alive: its sequence is a view over the stream's pooled blocks.
            var stream = this.body!;
            var committed = false;
            try
            {
                await input.CopyToAsync(stream, cancellationToken).ConfigureAwait(false);
                partition!.CommitEvent(new Event(DateTime.UtcNow, key!.Value, stream.GetReadOnlySequence(), stream));
                committed = true;
                return PublishResult.Success;
            }
            finally
            {
                if (!committed)
                {
                    // Blocks go back to the pool now rather than waiting for finalization.
                    stream.Dispose();
                }

                // In a finally so an abandoned publish does not leak the writer or what it holds.
                writerPool.Return(this);
            }
        }
    }

    private sealed class EventWriterPoolPolicy : PooledObjectPolicy<EventWriter>
    {
        public override EventWriter Create() => new EventWriter();

        public override bool Return(EventWriter obj) => obj.Release();
    }
    
    private void CommitEvent(Event @event) => this.log.Append(@event);
    
    public IEventWriter GetEventWriter(RefBytes key, long? size)
    {
        var writer = writerPool.Get();
        writer.Initialize(this, key, size);
        return writer;
    }
}
