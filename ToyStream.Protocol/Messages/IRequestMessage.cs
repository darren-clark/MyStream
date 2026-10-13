using System.IO.Pipelines;

namespace ToyStream.Protocol;

public interface IRequestMessage
{
    internal static abstract MessageType Type { get; }
}

public interface IStreamingRequestMessage : IRequestMessage
{
    public int? Size { get; }
    public PipeReader Body { get; }
}

public interface IRequestMessage<TSelf> : IMessage<TSelf>, IRequestMessage
    where TSelf : struct, IRequestMessage<TSelf>, allows ref struct;

public interface IStreamingRequestMessage<TSelf>: IMessage<TSelf>, IStreamingRequestMessage
    where TSelf : struct, IStreamingRequestMessage<TSelf>, allows ref struct;

public interface IRequestMessage<TSelf, TResponseMessage> : IMessage<TSelf>, IRequestMessage
    where TSelf : struct, IRequestMessage<TSelf, TResponseMessage>, allows ref struct
    where TResponseMessage : struct, IResponseMessage<TResponseMessage>, allows ref struct;

public interface IStreamingRequestMessage<TSelf, TResponseMessage> :IMessage<TSelf>, IStreamingRequestMessage
    where TSelf : struct, IStreamingRequestMessage<TSelf, TResponseMessage>, allows ref struct
    where TResponseMessage : struct, IResponseMessage<TResponseMessage>, allows ref struct;

