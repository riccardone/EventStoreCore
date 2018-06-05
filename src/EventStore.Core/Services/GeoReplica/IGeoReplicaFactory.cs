using System.Collections.Generic;

namespace EventStore.Core.Services.GeoReplica
{
    public interface IGeoReplicaFactory
    {
        string Name { get; }
        Dictionary<string, IGeoReplica> Create();
    }
}
