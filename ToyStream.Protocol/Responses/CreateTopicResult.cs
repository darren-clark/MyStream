namespace ToyStream.Protocol.Responses;

public enum CreateTopicResult
{
    Success,
    DuplicateTopic,
    InvalidPartitionCount
}
