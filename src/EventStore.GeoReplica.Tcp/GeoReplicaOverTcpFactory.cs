using System;
using System.Collections.Generic;
using EventStore.ClientAPI;
using EventStore.Core.Services.GeoReplica;

namespace EventStore.GeoReplica.Tcp
{
    public class GeoReplicaOverTcpFactory : IGeoReplicaFactory
    {
        public string Name => "GeoReplica-Tcp";
        public Dictionary<string, IGeoReplica> Create()
        {
            var connection = EventStoreConnection.Create(new Uri("ConnectTo=tcp://admin:changeit@localhost:1113"), "test-todo");
            connection.ConnectAsync().Wait();
            return new Dictionary<string, IGeoReplica> {{"TCP", new GeoReplicaOverTcp(Name, connection) }};
        }
    }
}
