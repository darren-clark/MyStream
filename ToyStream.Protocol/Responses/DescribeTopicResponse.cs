using ToyStream.Protocol.Messages;
using ToyStream.Shared;

namespace ToyStream.Protocol.Responses;

/// <summary>
/// A topic's partitions as the awaiting caller sees them. The descriptions are copied out because
/// the message views the connection's buffer, which is gone by the time the caller is resumed.
/// </summary>
public sealed class DescribeTopicResponse(DescribeTopicResult result, PartitionDescription[] partitions)
    : IResponse<DescribeTopicResponseMessage, DescribeTopicResponse>
{
    public DescribeTopicResult Result { get; } = result;

    /// <summary>Indexed by partition id, which is the index the server describes them in.</summary>
    public ReadOnlySpan<PartitionDescription> Partitions => partitions;

    static DescribeTopicResponse IResponse<DescribeTopicResponseMessage, DescribeTopicResponse>.CreateResponse(
        ref readonly DescribeTopicResponseMessage response)
    {
        var partitions = new PartitionDescription[response.Partitions.Count];
        response.Partitions.CopyTo(partitions);
        return new DescribeTopicResponse(response.Result, partitions);
    }
}
