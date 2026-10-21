namespace ToyStream.Protocol.Messages;

using ToyStream.Protocol.Responses;

/// <summary>
/// One fetch covers every topic and partition a consumer is waiting on. It has to: the connection
/// allows one outstanding request, so a long poll parked on a subset would starve the rest.
/// </summary>
public readonly ref struct FetchRequestMessage : IRequestMessage<FetchRequestMessage, FetchResponseMessage>
{
    public static MessageType Type => MessageType.Fetch;

    /// <summary>How long the server may hold the request open waiting for data.</summary>
    public required int MaxWaitMilliseconds { get; init; }

    public required int MaxBytes { get; init; }

    public required TopicNames Topics { get; init; }

    public required FetchEntries Entries { get; init; }

    static bool IMessage<FetchRequestMessage>.TryRead<TReader>(ref TReader reader, out FetchRequestMessage message,
        out int bytesToRead)
    {
        var tryReader = reader;
        if (!tryReader.TryReadInt32(out var maxWait, out bytesToRead) ||
            !tryReader.TryReadInt32(out var maxBytes, out bytesToRead) ||
            !TopicNames.TryRead(ref tryReader, out var topics, out bytesToRead) ||
            !FetchEntries.TryRead(ref tryReader, out var entries, out bytesToRead))
        {
            message = default;
            return false;
        }

        reader = tryReader;
        message = new FetchRequestMessage
        {
            MaxWaitMilliseconds = maxWait,
            MaxBytes = maxBytes,
            Topics = topics,
            Entries = entries
        };
        return true;
    }

    public void Write<TWriter>(TWriter writer) where TWriter : struct, IPrimitiveWriter, allows ref struct
    {
        writer.Write(MaxWaitMilliseconds);
        writer.Write(MaxBytes);
        Topics.Write(writer);
        Entries.Write(writer);
    }
}

/// <summary>
/// Only the header: the records follow it as a streamed body, so neither their number nor the size
/// of any one of them has to be known before the response starts going out.
/// </summary>
public readonly ref struct FetchResponseMessage : IResponseMessage<FetchResponseMessage>
{
    public static ResponseType Type => ResponseType.Fetch;

    public required FetchResult Result { get; init; }

    /// <summary>Carried again rather than referencing the request, so a response parses alone.</summary>
    public required TopicNames Topics { get; init; }

    static bool IMessage<FetchResponseMessage>.TryRead<TReader>(ref TReader reader, out FetchResponseMessage message,
        out int bytesToRead)
    {
        var tryReader = reader;
        if (!tryReader.TryReadEnum<FetchResult>(out var result, out bytesToRead))
        {
            message = default;
            return false;
        }

        // Mirrors Write: a failure carries no topic table. The body still follows, empty.
        if (result != FetchResult.Success)
        {
            reader = tryReader;
            message = new FetchResponseMessage { Result = result, Topics = default };
            return true;
        }

        if (!TopicNames.TryRead(ref tryReader, out var topics, out bytesToRead))
        {
            message = default;
            return false;
        }

        reader = tryReader;
        message = new FetchResponseMessage { Result = result, Topics = topics };
        return true;
    }

    public void Write<TWriter>(TWriter writer) where TWriter : struct, IPrimitiveWriter, allows ref struct
    {
        writer.Write(Result);
        if (Result != FetchResult.Success)
        {
            return;
        }

        Topics.Write(writer);
    }
}
