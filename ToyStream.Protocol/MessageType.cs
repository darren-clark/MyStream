namespace ToyStream.Protocol;

public enum MessageType: byte
{
    CreateTopic = 1,
    Publish = 2,
}