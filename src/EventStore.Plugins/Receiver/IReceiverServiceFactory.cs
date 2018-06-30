using System.Collections.Generic;

namespace EventStore.Plugins.Receiver
{
    public interface IReceiverServiceFactory
    {
        string Name { get; }
        IList<IReceiverService> Create();
    }
}
