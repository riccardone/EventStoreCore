using System.Collections.Generic;

namespace EventStore.Core.Services.GeoReplica
{
    public interface IGeoReplicaFactory
    {
        string Name { get; }
        KeyValuePair<string, IGeoReplica> Create();
    }
}
