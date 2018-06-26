using System.Collections;
using System.Collections.Generic;
using EventStore.Core.Bus;
using EventStore.Core.Data;
using EventStore.Core.Messages;
using EventStore.Plugins.Dispatcher;

namespace EventStore.Core.Services.GeoReplica
{
    public class DispatcherHostService : IHandle<SystemMessage.StateChangeMessage>
    {
        private readonly IDispatcherServiceFactory _dispatcherFactory;
        private IList<IDispatcherService> _dispatcherServices;

        public DispatcherHostService(Plugins.Dispatcher.IDispatcherServiceFactory subscriberFactory)
        {
            _dispatcherFactory = subscriberFactory;
        }

        public void Handle(SystemMessage.StateChangeMessage message)
        {
            if (message.State != VNodeState.Master && message.State != VNodeState.Clone &&
                message.State != VNodeState.Slave) return;
            _dispatcherServices = _dispatcherFactory.Create();
            foreach (var dispatcherService in _dispatcherServices)
            {
                dispatcherService.Start();
            }
        }
    }
}
