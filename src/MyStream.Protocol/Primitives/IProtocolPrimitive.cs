namespace MyStream.Protocol.Primitives;

public interface IProtocolPrimitive<T> where T: struct, IProtocolPrimitive<T>, allows ref struct
{
    internal static abstract bool TryRead<TReader>(scoped ref TReader reader, out T value, out int bytesToRead)
        where TReader : IPrimitiveReader, allows ref struct;
    internal void Write<TWriter>(TWriter writer) where TWriter : struct, IPrimitiveWriter, allows ref struct;
}