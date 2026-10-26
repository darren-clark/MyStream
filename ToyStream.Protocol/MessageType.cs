namespace ToyStream.Protocol;

/// <summary>Requests: the high bit is clear, so <see cref="ConnectionBase"/> can route on the raw byte.</summary>
public enum MessageType: byte
{
    CreateTopic = 1,
    PublishEvent = 2,
    Subscribe = 3,
    DescribeTopic = 4,
    Fetch = 5,

    /// <summary>Server to client: a batch of records for a subscription, as a streamed body.</summary>
    PushRecords = 6,

    /// <summary>Server to client: the subscription cannot continue, so re-subscribe.</summary>
    SubscriptionLost = 7
}

/// <summary>Responses: the high bit is set.</summary>
public enum ResponseType: byte
{
    Acknowledge = ProtocolConstants.ResponseFlag,
    AcknowledgeWithResult = ProtocolConstants.ResponseFlag | 1,
    DescribeTopic = ProtocolConstants.ResponseFlag | 2,
    Fetch = ProtocolConstants.ResponseFlag | 4,
}

public static class ProtocolConstants
{
    public const byte ResponseFlag = 0x80;

    /// <summary>
    /// A chunk header is four bytes taken on trust from the wire, so it is bounded: a corrupt length
    /// fails immediately instead of consuming the following messages as payload.
    /// </summary>
    public const int MaxChunkSize = 1024 * 1024;

    /// <summary>
    /// A string length is two bytes taken on trust from the wire, so a negative one must be rejected
    /// rather than reaching the reader, which slices by it.
    /// </summary>
    public const int MaxStringSize = short.MaxValue;

    /// <summary>
    /// Partition keys are chosen by the sender and only ever hashed or stored whole, so they are
    /// capped small enough that a receiver can flatten one onto the stack.
    /// </summary>
    public const int MaxKeySize = 1024;

    /// <summary>
    /// A describe response carries one fixed size record per partition, so the count is bounded:
    /// it decides how much a receiver reads, and how much it may flatten onto the stack.
    /// </summary>
    public const int MaxPartitions = 1024;

    /// <summary>
    /// A fetch names its topics once and its entries reference them by index, so both the table and
    /// the entry list are bounded before either is read.
    /// </summary>
    public const int MaxTopicsPerFetch = 64;

    public const int MaxFetchEntries = 1024;

    public const int MaxFetchRecords = 4096;

    public static bool IsResponse(byte messageType) => (messageType & ResponseFlag) != 0;
}