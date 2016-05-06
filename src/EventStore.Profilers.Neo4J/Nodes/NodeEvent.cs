using System;
using System.Collections.Generic;
using System.Linq;
using System.Text;

namespace EventStore.Profilers.Neo4J.Nodes
{
    public class NodeEvent
    {
        public string EventType { get; set; }
        public string Id { get; set; }
        public string StreamId { get; set; }
        public dynamic Data { get; set; }
    }
}
