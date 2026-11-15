using System.IO.Pipelines;

using MyStream.Protocol.Messages;

namespace MyStream.Protocol.Responses;

// The heap projection of a response message. Built by the awaiting side only, while the
// pipe buffer the response message views is still valid.
public interface IResponse<TResponseMessage, out TResponse>
    where TResponseMessage : struct, IResponseMessage<TResponseMessage>, allows ref struct
    where TResponse : IResponse<TResponseMessage, TResponse>
{
    internal static abstract TResponse CreateResponse(ref readonly TResponseMessage response);
}

/// <summary>
/// A response whose body is streamed rather than parsed from the buffer. The awaiting caller reads
/// it, and the connection cannot continue until it says it is finished.
/// </summary>
public interface IStreamingResponse
{
    ValueTask BodyDrained { get; }
}

/// <summary>
/// The streamed counterpart of <see cref="IResponse{TResponseMessage,TResponse}"/>: built from the
/// header while the buffer is valid, then given the body to read.
/// </summary>
public interface IStreamingResponse<TResponseMessage, out TResponse> : IStreamingResponse
    where TResponseMessage : struct, IResponseMessage<TResponseMessage>, allows ref struct
    where TResponse : IStreamingResponse<TResponseMessage, TResponse>
{
    internal static abstract TResponse CreateResponse(ref readonly TResponseMessage header, PipeReader body);
}
