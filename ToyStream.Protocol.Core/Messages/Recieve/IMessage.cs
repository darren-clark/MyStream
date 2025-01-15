namespace ToyStream.Protocol.Messages.Recieve;

public interface IMessage<T> where T: struct, IMessage<T>, allows ref struct
{
    
}