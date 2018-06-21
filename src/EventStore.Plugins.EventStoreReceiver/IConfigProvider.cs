using EventStore.Plugins.EventStoreReceiver.Config;

namespace EventStore.Plugins.EventStoreReceiver
{
    public interface IConfigProvider
    {
        Root GetSettings();
    }
}
