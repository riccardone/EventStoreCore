using System;

namespace EventStore.Profilers.ElasticSearch.Documents
{
    public class NodeEvent
    {
        public string EventType { get; set; }
        public string Id { get; set; }
        public string StreamId { get; set; }
        public dynamic Data { get; set; }
        public DateTime TimeStamp { get; set; }
    }
}
