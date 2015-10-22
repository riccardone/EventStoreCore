using EventStore.Common.Utils;
using EventStore.Core.Bus;
using EventStore.Core.Messages;
using EventStore.Core.Messaging;

namespace EventStore.Core.Services.Transport.Amqp
{
    public class SendOverAmqpEnvelope : IEnvelope
    {
        private readonly IPublisher _networkSendQueue;
        private readonly AmqpConnectionManager _manager;

        public SendOverAmqpEnvelope(AmqpConnectionManager manager, IPublisher networkSendQueue)
        {
            Ensure.NotNull(manager, "manager");
            Ensure.NotNull(networkSendQueue, "networkSendQueue");
            _networkSendQueue = networkSendQueue;
            _manager = manager;
        }

        public void ReplyWith<T>(T message) where T : Message
        {
            _networkSendQueue.Publish(new AmqpMessage.Send(_manager, message));
        }
    }
}