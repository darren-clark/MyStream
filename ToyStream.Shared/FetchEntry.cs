namespace ToyStream.Shared;

using System.Buffers;

/// <summary>One partition a consumer wants to read, and where it wants to read from.</summary>
public readonly record struct FetchEntry(short TopicIndex, short PartitionId, long Offset);

/// <summary>
/// A record as it travels in a fetch response. Deliberately not a ref struct: these live in spans
/// and arrays while a response is assembled, which a ref struct cannot do.
/// </summary>
public readonly record struct FetchedRecord(
    short TopicIndex,
    short PartitionId,
    long Offset,
    DateTime Timestamp,
    ReadOnlySequence<byte> Key,
    ReadOnlySequence<byte> Body);

/// <summary>
/// Everything about a fetched record except its body, which a reader streams separately rather than
/// holding whole.
/// </summary>
public readonly record struct FetchedRecordHeader(
    short TopicIndex,
    short PartitionId,
    long Offset,
    DateTime Timestamp);
