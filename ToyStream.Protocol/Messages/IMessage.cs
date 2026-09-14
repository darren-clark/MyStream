namespace ToyStream.Protocol;

public interface IMessage
{
}

public interface IMessage<T>: IMessage where T : struct, IMessage<T>, allows ref struct
{
    internal MessageType Type { get; }

    internal static abstract bool TryRead<TReader>(ref TReader reader, out T message, out int bytesToRead)
        where TReader : IPrimitiveReader, allows ref struct;

    internal void Write<TWriter>(TWriter writer) where TWriter : struct, IPrimitiveWriter, allows ref struct;
}