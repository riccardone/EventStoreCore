using EventStore.Plugins.EventStoreDispatcher.Config;

namespace EventStore.Plugins.EventStoreDispatcher
{
    public interface IConfigProvider
    {
        Root GetSettings();
    }
}
