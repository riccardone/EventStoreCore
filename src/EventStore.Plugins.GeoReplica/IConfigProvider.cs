using EventStore.Plugins.GeoReplica.Config;

namespace EventStore.Plugins.GeoReplica
{
    public interface IConfigProvider
    {
        Root GetSettings();
    }
}
