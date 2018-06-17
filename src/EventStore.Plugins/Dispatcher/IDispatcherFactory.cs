using System.Collections.Generic;

namespace EventStore.Plugins.Dispatcher
{
    public interface IDispatcherFactory
    {
        string Name { get; }
        IDictionary<string, IDispatcher> Create();
    }
}
