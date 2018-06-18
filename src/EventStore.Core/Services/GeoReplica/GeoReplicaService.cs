using EventStore.Core.Bus;
using EventStore.Core.Data;
using EventStore.Core.Messages;

namespace EventStore.Core.Services.GeoReplica
{
    public class GeoReplicaService : IHandle<SystemMessage.StateChangeMessage>
    {
        private readonly Plugins.Dispatcher.ISubscriberServiceFactory _subscriberFactory;
        private Plugins.Dispatcher.ISubscriberService _subscriber;

        public GeoReplicaService(Plugins.Dispatcher.ISubscriberServiceFactory subscriberFactory)
        {
            _subscriberFactory = subscriberFactory;
        }

        public void Handle(SystemMessage.StateChangeMessage message)
        {
            if (message.State != VNodeState.Master && message.State != VNodeState.Clone &&
                message.State != VNodeState.Slave) return;
            _subscriber = _subscriberFactory.Create();
            _subscriber.Start();
        }
    }
}
