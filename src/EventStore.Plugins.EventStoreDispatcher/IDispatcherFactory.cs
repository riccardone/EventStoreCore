using System.Collections.Generic;

namespace EventStore.Plugins.EventStoreDispatcher
{
    public interface IDispatcherFactory
    {
        string Name { get; }
        IDictionary<string, IDispatcher> Create();
    }
}
