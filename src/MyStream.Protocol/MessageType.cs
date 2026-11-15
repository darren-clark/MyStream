namespace MyStream.Protocol;

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
    SubscriptionLost = 7,

    /// <summary>Client to server: how far a subscriber has processed each of its partitions.</summary>
    CommitOffsets = 8,

    /// <summary>
    /// Server to client: these partitions are being taken back. A position in the stream rather than
    /// a notification - nothing for them follows it, so it fixes what the final offset can be.
    /// </summary>
    Revoke = 9,

    /// <summary>Client to server: the answer to a <see cref="Revoke"/>, carrying where it stopped.</summary>
    RevokeComplete = 10,

    /// <summary>
    /// Client to server: going on purpose rather than going away. Separates a shutdown from a lost
    /// connection, which the group would otherwise have to wait out in case it came back.
    /// </summary>
    LeaveGroup = 11,
    InitProducer = 12,
}

/// <summary>Responses: the high bit is set.</summary>
public enum ResponseType: byte
{
    Acknowledge = ProtocolConstants.ResponseFlag,
    AcknowledgeWithResult = ProtocolConstants.ResponseFlag | 1,
    DescribeTopic = ProtocolConstants.ResponseFlag | 2,
    Fetch = ProtocolConstants.ResponseFlag | 3,
    InitProducer = ProtocolConstants.ResponseFlag | 4,
}