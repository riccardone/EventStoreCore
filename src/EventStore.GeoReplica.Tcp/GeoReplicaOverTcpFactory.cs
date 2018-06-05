using System;
using System.Collections.Generic;
using EventStore.Core.Services.GeoReplica;

namespace EventStore.GeoReplica.Tcp
{
    public class GeoReplicaOverTcpFactory : IGeoReplicaFactory
    {
        public string Name { get; }
        public Dictionary<string, IGeoReplica> Create()
        {
            return new Dictionary<string, IGeoReplica> {{"TCP", new GeoReplicaOverTcp()}};
        }
    }
}
