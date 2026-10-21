namespace ToyStream.Protocol.Responses;

public enum FetchResult : byte
{
    Success,
    UnknownTopic,
    UnknownPartition,
    OffsetOutOfRange
}
