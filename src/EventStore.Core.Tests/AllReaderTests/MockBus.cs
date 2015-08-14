using System;
using System.Collections.Generic;
using System.Linq;
using EventStore.Common.Log;
using EventStore.Common.Utils;
using EventStore.Core.Bus;
using EventStore.Core.Messaging;

namespace EventStore.Core.Tests.AllReaderTests
{
    internal class MockBus : IBus, IThreadSafePublisher, IHandle<Message>
    {
        public static readonly TimeSpan DefaultSlowMessageThreshold = TimeSpan.FromMilliseconds(48);

        public string Name { get { return "Mock Bus"; } }
        
        private readonly List<IMessageHandler>[] _handlers;

        public MockBus()
        {
            _handlers = new List<IMessageHandler>[MessageHierarchy.MaxMsgTypeId + 1];
            for (var i = 0; i < _handlers.Length; ++i)
            {
                _handlers[i] = new List<IMessageHandler>();
            }
        }

        public void Subscribe<T>(IHandle<T> handler) where T : Message
        {
            Ensure.NotNull(handler, "handler");

            var descendants = MessageHierarchy.DescendantsByType[typeof(T)];
            for (var i = 0; i < descendants.Length; ++i)
            {
                var handlers = _handlers[descendants[i]];
                if (!handlers.Any(x => x.IsSame<T>(handler)))
                    handlers.Add(new MessageHandler<T>(handler, handler.GetType().Name));
            }
        }

        public void Unsubscribe<T>(IHandle<T> handler) where T : Message
        {
            Ensure.NotNull(handler, "handler");

            var descendants = MessageHierarchy.DescendantsByType[typeof(T)];
            for (var i = 0; i < descendants.Length; ++i)
            {
                var handlers = _handlers[descendants[i]];
                var messageHandler = handlers.FirstOrDefault(x => x.IsSame<T>(handler));
                if (messageHandler != null)
                    handlers.Remove(messageHandler);
            }
        }

        public void Handle(Message message)
        {
            Publish(message);
        }

        public void Publish(Message message)
        {
            var handlers = _handlers[message.MsgTypeId];
            if (handlers.Count == 0)
            {
                System.Diagnostics.Debug.WriteLine("Not Handling : {0} ", message.GetType().Name);
            }
            for (int i = 0, n = handlers.Count; i < n; ++i)
            {
                var handler = handlers[i];
                System.Diagnostics.Debug.WriteLine("Handling : {0} -> {1}", handler.GetType().Name, message.GetType().Name);
                handler.TryHandle(message);
            }
        }
    }
}
