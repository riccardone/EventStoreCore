using EventStore.Plugins.EventStoreDispatcher.Config;

namespace EventStore.Plugins.EventStoreDispatcher
{
    public interface IDispatcherFactory
    {
        string Name { get; }
        IDispatcher Create(Destination destination);
    }
}
