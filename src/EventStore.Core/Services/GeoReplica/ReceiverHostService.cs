using System;
using System.Threading;
using EventStore.Common.Log;
using EventStore.Core.Bus;
using EventStore.Core.Data;
using EventStore.Core.Messages;

namespace EventStore.Core.Services.GeoReplica
{
    public class ReceiverHostService : IHandle<SystemMessage.StateChangeMessage>
    {
        private static readonly ILogger Log = LogManager.GetLoggerFor<ReceiverHostService>();
        private readonly Plugins.Receiver.IReceiverServiceFactory _receiverServiceFactory;
        private Plugins.Receiver.IReceiverService _receiverService;

        public ReceiverHostService(Plugins.Receiver.IReceiverServiceFactory receiverServiceFactory)
        {
            _receiverServiceFactory = receiverServiceFactory;
        }

        public void Handle(SystemMessage.StateChangeMessage message)
        {
            if (message.State != VNodeState.Master && message.State != VNodeState.Clone &&
                message.State != VNodeState.Slave) return;
            try
            {
                var t = new Thread(Start) { IsBackground = true };
                t.Start();
            }
            catch (Exception e)
            {
                Log.ErrorException(e, "Error on ReceiverHostService");
            }
        }

        private void Start()
        {
            Thread.Sleep(10000);
            if (_receiverServiceFactory == null)
                return;
            _receiverService = _receiverServiceFactory.Create();
            _receiverService?.Start();
        }
    }
}
