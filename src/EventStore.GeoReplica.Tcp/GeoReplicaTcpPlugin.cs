using System.ComponentModel.Composition;
using EventStore.Core.PluginModel;
using EventStore.Core.Services.GeoReplica;

namespace EventStore.GeoReplica.Tcp
{
    [Export(typeof(IGeoReplicaPlugin))]
    public class GeoReplicaTcpPlugin : IGeoReplicaPlugin
    {
        public string Name => "GeoReplica TCP Plugin";
        public string Version => "1.0";
        public IGeoReplicaFactory GetGeoReplicaFactory()
        {
            return new GeoReplicaOverTcpFactory();
        }
    }
}
