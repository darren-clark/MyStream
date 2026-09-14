namespace ToyStream.Protocol;

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