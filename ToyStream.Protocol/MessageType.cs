namespace ToyStream.Protocol;

/// <summary>Requests: the high bit is clear, so <see cref="Connection"/> can route on the raw byte.</summary>
public enum MessageType: byte
{
    CreateTopic = 1,
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

    public static bool IsResponse(byte messageType) => (messageType & ResponseFlag) != 0;
}