using System.Threading.Tasks;
using EventStore.Core.Data;
using EventStore.Core.Services.GeoReplica;

namespace EventStore.GeoReplica.Tcp
{
    public class GeoReplicaOverTcp : IGeoReplica
    {
        public string Name { get; }

        public async Task DispatchAsynch(long version, ResolvedEvent evt, byte[] metadata)
        {
            // TODO how to get a IEventStoreConnection to a remote destination?
            // TODO AppendToStream...
            return;
        }
    }
}
