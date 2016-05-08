using System;
using System.Net;
using EventStore.Core.Data;
using EventStore.Core.Services.EventProfiler.Strategy;
using EventStore.Profilers.Neo4J.Nodes;
using Newtonsoft.Json;

namespace EventStore.Profilers.Neo4J
{
    public class Neo4JProfilerStrategy : IEventProfilerStrategy
    {
        public string Name { get { return "Neo4JProfiler"; } }
        public void PushMessageToProfiler(string streamId, ResolvedEvent ev)
        {
            // this assume your neo4j-server.properties file authentication setting set to false
            // dbms.security.auth_enabled=false
            var urlReq = "http://localhost:7474/db/data";
            var obj = new NodeEvent
            {
                Id = ev.OriginalEvent.EventId.ToString(),
                EventType = ev.OriginalEvent.EventType,
                StreamId = ev.OriginalEvent.EventStreamId,
                Data = ev.OriginalEvent.DebugDataView
            };
            using (var client = new WebClient())
                client.UploadStringAsync(new Uri(urlReq), "POST", JsonConvert.SerializeObject(obj)); 
        }
    }
}
