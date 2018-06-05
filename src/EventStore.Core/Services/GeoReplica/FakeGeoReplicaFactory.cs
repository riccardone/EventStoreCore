using System;
using System.Collections.Generic;

namespace EventStore.Core.Services.GeoReplica
{
    public class FakeGeoReplicaFactory : IGeoReplicaFactory
    {
        public string Name => "FakeFactory";

        public Dictionary<string, IGeoReplica> Create()
        {
            throw new NotImplementedException();
        }
    }
}
