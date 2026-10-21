namespace ToyStream.Protocol.Responses;

/// <summary>
/// The describe response type is fixed by the request, so a failure is carried in the response
/// rather than answered with a different message.
/// </summary>
public enum DescribeTopicResult : byte
{
    Success,
    UnknownTopic
}
