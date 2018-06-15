using EventStore.Core.Services.GeoReplica;

namespace EventStore.Core.PluginModel
{
    public interface IGeoReplicaPlugin
    {
        string Name { get; }
        string Version { get; }
        IGeoReplicaFactory GetStrategyFactory();
    }
}
