namespace ToyStream.Protocol;

/// <summary>Requests: the high bit is clear, so <see cref="ConnectionBase"/> can route on the raw byte.</summary>
public enum MessageType: byte
{
    CreateTopic = 1,
    PublishEvent = 2,
}

/// <summary>Responses: the high bit is set.</summary>
public enum ResponseType: byte
{
    Acknowledge = ProtocolConstants.ResponseFlag,
    AcknowledgeWithResult = ProtocolConstants.ResponseFlag | 1,
}

public static class ProtocolConstants
{
    public const byte ResponseFlag = 0x80;

    /// <summary>
    /// A chunk header is four bytes taken on trust from the wire, so it is bounded: a corrupt length
    /// fails immediately instead of consuming the following messages as payload.
    /// </summary>
    public const int MaxChunkSize = 1024 * 1024;

    public static bool IsResponse(byte messageType) => (messageType & ResponseFlag) != 0;
}