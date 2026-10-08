namespace ToyStream.Protocol.Benchmark.Lookup;

using System.Collections.Frozen;
using System.IO.Pipelines;
using BenchmarkDotNet.Attributes;
using Messages;
using Protocol.Dispatch;
using Scale;

/// <summary>
/// Isolates the cost MapConnection adds per message: a map lookup, a cast to the closed delegate
/// type, and a delegate invoke. Measured on its own rather than inside a round trip, because a few
/// nanoseconds is not resolvable against ~600 ns of pipe traffic - that is the mistake the dispatch
/// comparisons kept making.
/// </summary>
[MemoryDiagnoser]
public class DispatchLookupBenchmarks
{
    private const int Operations = 1000;
    private const int RegisteredTypes = 30;

    private Dictionary<MessageType, object> dictionary = null!;
    private FrozenDictionary<MessageType, object> frozen = null!;
    private object?[] array = null!;
    private MessageHandler<ScaleMessage00, AcknowledgeMessage> handlerDelegate = null!;
    private PipeWriter writer = null!;

    private long counter;

    [GlobalSetup]
    public void Setup()
    {
        this.writer = new Pipe().Writer;
        this.handlerDelegate = this.Handle;

        // Populated with a realistic number of entries so hashing is not trivially cheap.
        this.dictionary = new Dictionary<MessageType, object>();
        this.array = new object?[256];

        for (var i = 0; i < RegisteredTypes; i++)
        {
            this.dictionary[(MessageType)(10 + i)] = this.handlerDelegate;
            this.array[10 + i] = this.handlerDelegate;
        }

        this.frozen = this.dictionary.ToFrozenDictionary();
    }

    private ValueTask Handle(in ScaleMessage00 message, in ResponseWriter<AcknowledgeMessage> responseWriter)
    {
        this.counter += message.Value;
        return default;
    }

    /// <summary>Floor: no delegate, no lookup.</summary>
    [Benchmark(OperationsPerInvoke = Operations, Baseline = true)]
    public long DirectCall()
    {
        var message = new ScaleMessage00 { Value = 1 };
        for (var i = 0; i < Operations; i++)
        {
            var responseWriter = new ResponseWriter<AcknowledgeMessage>(this.writer);
            _ = this.Handle(in message, in responseWriter);
        }

        return this.counter;
    }

    /// <summary>Delegate invoke, but no lookup and no cast.</summary>
    [Benchmark(OperationsPerInvoke = Operations)]
    public long DirectDelegate()
    {
        var message = new ScaleMessage00 { Value = 1 };
        for (var i = 0; i < Operations; i++)
        {
            var responseWriter = new ResponseWriter<AcknowledgeMessage>(this.writer);
            _ = this.handlerDelegate.Invoke(in message, in responseWriter);
        }

        return this.counter;
    }

    /// <summary>Array indexed by the message type byte, so no hashing.</summary>
    [Benchmark(OperationsPerInvoke = Operations)]
    public long ArrayLookup()
    {
        var message = new ScaleMessage00 { Value = 1 };
        for (var i = 0; i < Operations; i++)
        {
            var responseWriter = new ResponseWriter<AcknowledgeMessage>(this.writer);
            var handler = (MessageHandler<ScaleMessage00, AcknowledgeMessage>)this.array[(byte)ScaleMessage00.Type]!;
            _ = handler.Invoke(in message, in responseWriter);
        }

        return this.counter;
    }

    [Benchmark(OperationsPerInvoke = Operations)]
    public long FrozenDictionaryLookup()
    {
        var message = new ScaleMessage00 { Value = 1 };
        for (var i = 0; i < Operations; i++)
        {
            var responseWriter = new ResponseWriter<AcknowledgeMessage>(this.writer);
            var handler = (MessageHandler<ScaleMessage00, AcknowledgeMessage>)this.frozen[ScaleMessage00.Type];
            _ = handler.Invoke(in message, in responseWriter);
        }

        return this.counter;
    }

    /// <summary>What MapConnection does today.</summary>
    [Benchmark(OperationsPerInvoke = Operations)]
    public long DictionaryLookup()
    {
        var message = new ScaleMessage00 { Value = 1 };
        for (var i = 0; i < Operations; i++)
        {
            var responseWriter = new ResponseWriter<AcknowledgeMessage>(this.writer);
            var handler = (MessageHandler<ScaleMessage00, AcknowledgeMessage>)this.dictionary[ScaleMessage00.Type];
            _ = handler.Invoke(in message, in responseWriter);
        }

        return this.counter;
    }
}
