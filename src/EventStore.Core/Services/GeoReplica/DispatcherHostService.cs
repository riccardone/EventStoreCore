using EventStore.Core.Bus;
using EventStore.Core.Data;
using EventStore.Core.Messages;

namespace EventStore.Core.Services.GeoReplica
{
    public class DispatcherHostService : IHandle<SystemMessage.StateChangeMessage>
    {
        private readonly Plugins.Dispatcher.IDispatcherServiceFactory _dispatcherFactory;
        private Plugins.Dispatcher.IDispatcherService _dispatcherService;

        public DispatcherHostService(Plugins.Dispatcher.IDispatcherServiceFactory subscriberFactory)
        {
            _dispatcherFactory = subscriberFactory;
        }

        public void Handle(SystemMessage.StateChangeMessage message)
        {
            if (message.State != VNodeState.Master && message.State != VNodeState.Clone &&
                message.State != VNodeState.Slave) return;
            _dispatcherService = _dispatcherFactory.Create();
            _dispatcherService.Start();
        }
    }
}
