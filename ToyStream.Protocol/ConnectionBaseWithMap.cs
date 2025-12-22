namespace ToyStream.Protocol;

using System.Diagnostics.CodeAnalysis;
using System.Linq.Expressions;
using System.Reflection;
using Messages;

public abstract class ConnectionBaseWithMap : IAsyncDisposable
{
    protected ConnectionBaseWithMap()
    {
        
    }

    
    protected void Start(IMessageHandlerMap messageHandlerMap)
    {
    }

    protected delegate void ResponseWriter<TResponseMessage>(ref readonly TResponseMessage response)
        where TResponseMessage : struct, IResponseMessage<TResponseMessage>, allows ref struct;

    protected delegate ValueTask MessageHandler<TMessage, TResponseMessage>(ref readonly TMessage message,
        ResponseWriter<TResponseMessage> responseWriter)
        where TMessage : struct, IRequestMessage<TMessage, TResponseMessage>, allows ref struct
        where TResponseMessage : struct, IResponseMessage<TResponseMessage>, allows ref struct;


    protected interface IMessageHandlerMap
    {
        public bool TryGetHandler<TMessage, TResponseMessage>(
            [NotNullWhen(true)] out MessageHandler<TMessage, TResponseMessage>? handler)
            where TMessage : struct, IRequestMessage<TMessage, TResponseMessage>
            where TResponseMessage : struct, IResponseMessage<TResponseMessage>;
    }

    protected class MessageHandlerMapBuilder<THandler>(THandler handler)
    {
        private readonly Dictionary<Type, Delegate> handlers = new();
        
        
        public MessageHandlerMapBuilder<THandler> Register<TMessage, TResponseMessage>(
            MessageHandler<TMessage, TResponseMessage> handler)
            where TMessage : struct, IRequestMessage<TMessage, TResponseMessage>, allows ref struct
            where TResponseMessage : struct, IResponseMessage<TResponseMessage>, allows ref struct
        {
            this.handlers.Add(typeof(TMessage), handler);
            return this;
        }

        public MessageHandlerMapBuilder<THandler> Register(string methodName)
        {
            var method = typeof(THandler).GetMethod(methodName, BindingFlags.Instance | BindingFlags.Static | BindingFlags.Public | BindingFlags.NonPublic) ?? throw new InvalidOperationException($"Method {methodName} not found in {typeof(THandler).Name}");
            var parameters = method.GetParameters();
            if (parameters.Length != 2)
            {
                throw new ArgumentException("Method must have two parameters");
            }

            var messageType = parameters[0].ParameterType;
            var responseType = parameters[1].ParameterType;

            if (!typeof(IRequestMessage<,>).IsAssignableFrom(messageType))
            {
                throw new ArgumentException("First parameter must be a request message");
            }

            if (!typeof(IResponseMessage<>).IsAssignableFrom(responseType))
            {
                throw new ArgumentException("Second parameter must be a response message");
            }

            var delegateType = typeof(MessageHandler<,>).MakeGenericType(messageType, responseType);
            this.handlers.Add(messageType, Delegate.CreateDelegate(typeof(THandler), handler, method));
            return this;    
        }
        
        public IMessageHandlerMap Build() => new MessageHandlerMap(this.handlers.AsReadOnly());

        private class MessageHandlerMap : IMessageHandlerMap
        {
            private readonly IReadOnlyDictionary<Type, Delegate> handlers;

            public MessageHandlerMap(IReadOnlyDictionary<Type, Delegate> handlers)
            {
                this.handlers = handlers;
            }

            public bool TryGetHandler<TMessage, TResponseMessage>(
                [NotNullWhen(true)] out MessageHandler<TMessage, TResponseMessage>? handler)
                where TMessage : struct, IRequestMessage<TMessage, TResponseMessage>
                where TResponseMessage : struct, IResponseMessage<TResponseMessage>
            {
                if (this.handlers.TryGetValue(typeof(TMessage), out var untypedHandler))
                {
                    handler = (MessageHandler<TMessage, TResponseMessage>)untypedHandler;
                    return true;
                }

                handler = default;
                return false;
            }
        }
    }

    public async ValueTask DisposeAsync()
    {
        // TODO release managed resources here
    }
}

public class ConnectionWithMap : ConnectionBaseWithMap
{
    public ConnectionWithMap()
    {
        this.Start(new MessageHandlerMapBuilder<ConnectionWithMap>(this)
            .Register( nameof(this.HandleCreateTopic))
            .Build());
    }

    private ValueTask HandleCreateTopic(ref readonly CreateTopicMessage message,
        ResponseWriter<CreateTopicResponseMessage> responsewriter)
    {
        var result = new CreateTopicResponseMessage() { Result = CreateTopicResult.TopicCreated };
        responsewriter(in result);
        return default;
    }
}