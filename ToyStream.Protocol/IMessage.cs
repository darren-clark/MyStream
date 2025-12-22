namespace ToyStream.Protocol;

using System.Diagnostics.Contracts;

public interface IMessage
{
    internal MessageType Type { get; }
    
    internal void Write<TWriter>(TWriter writer) where TWriter : struct, IPrimitiveWriter, allows ref struct;
}

public interface IMessage<T>: IMessage where T : struct, IMessage<T>, allows ref struct
{
    internal static abstract bool TryRead<TReader>(ref TReader reader, out T message, out int bytesToRead)
        where TReader : IPrimitiveReader, allows ref struct;
}

public interface IRequestMessage<TRequest, TResponseMessage> : IMessage<TRequest>
    where TRequest : struct, IRequestMessage<TRequest, TResponseMessage>, allows ref struct
    where TResponseMessage : struct, IResponseMessage<TResponseMessage>, allows ref struct
{
}

public interface IResponseMessage<TResponseMessage> : IMessage<TResponseMessage>
    where TResponseMessage : struct, IResponseMessage<TResponseMessage>, allows ref struct
{
}

public interface IResponse
{
    
}

public interface IResponse<TResponseMessage, out TResponse>: IResponse 
    where TResponseMessage : struct, IResponseMessage<TResponseMessage>, allows ref struct
    where TResponse : class, IResponse<TResponseMessage, TResponse>    
{
    internal static abstract TResponse CreateResponse(ref readonly TResponseMessage response);
}