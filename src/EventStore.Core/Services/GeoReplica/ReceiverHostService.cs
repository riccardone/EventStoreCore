using EventStore.Core.Bus;
using EventStore.Core.Data;
using EventStore.Core.Messages;

namespace EventStore.Core.Services.GeoReplica
{
    public class ReceiverHostService : IHandle<SystemMessage.StateChangeMessage>
    {
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
            _receiverService = _receiverServiceFactory.Create();
            _receiverService.Start();
        }
    }
}
