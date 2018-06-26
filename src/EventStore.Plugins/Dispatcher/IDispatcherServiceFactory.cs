using System.Collections.Generic;

namespace EventStore.Plugins.Dispatcher
{
    public interface IDispatcherServiceFactory
    {
        IList<IDispatcherService> Create();
    }
}
