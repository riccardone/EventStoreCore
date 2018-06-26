using System;
using System.Collections.Generic;
using System.Threading;
using EventStore.Common.Log;
using EventStore.Core.Bus;
using EventStore.Core.Data;
using EventStore.Core.Messages;
using EventStore.Plugins.Dispatcher;

namespace EventStore.Core.Services.GeoReplica
{
    public class DispatcherHostService : IHandle<SystemMessage.StateChangeMessage>
    {
        private static readonly ILogger Log = LogManager.GetLoggerFor<DispatcherHostService>();
        private readonly IDispatcherServiceFactory _dispatcherFactory;
        private IList<IDispatcherService> _dispatcherServices;

        public DispatcherHostService(IDispatcherServiceFactory subscriberFactory)
        {
            _dispatcherFactory = subscriberFactory;
        }

        public void Handle(SystemMessage.StateChangeMessage message)
        {
            if (message.State != VNodeState.Master && message.State != VNodeState.Clone &&
                message.State != VNodeState.Slave) return;
            try
            {
                var t = new Thread(StartService) { IsBackground = true };
                t.Start();
            }
            catch (Exception e)
            {
                Log.ErrorException(e, "Error on DispatcherHostService");
            }
        }

        private void StartService()
        {
            if (_dispatcherFactory == null)
                return;
            _dispatcherServices = _dispatcherFactory.Create();
            foreach (var dispatcherService in _dispatcherServices)
            {
                dispatcherService.Start();
            }
        }
    }
}
