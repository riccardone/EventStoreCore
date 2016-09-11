using System;
using System.Net;
using EventStore.Core.Data;
using EventStore.Core.Services.EventProfiler.Strategy;
using EventStore.Profilers.ElasticSearch.Documents;
using Newtonsoft.Json;

namespace EventStore.Profilers.ElasticSearch
{
    public class ElasticSearchProfilerStrategy : IEventProfilerStrategy
    {
        public string Name { get { return "ElasticSearchProfiler"; } }
        public void PushMessageToProfiler(string streamId, ResolvedEvent ev)
        {
            var urlReq = string.Format("http://localhost:9200/eventstore/{0}/{1}", ev.Event.EventType, ev.Event.EventId);
            var obj = new NodeEvent
            {
                Id = ev.OriginalEvent.EventId.ToString(),
                EventType = ev.OriginalEvent.EventType,
                StreamId = ev.OriginalEvent.EventStreamId,
                Data = ev.OriginalEvent.DebugDataView,
                TimeStamp = ev.OriginalEvent.TimeStamp
            };
            using (var client = new WebClient())
                client.UploadStringAsync(new Uri(urlReq), "PUT", JsonConvert.SerializeObject(obj));
        }

        private void CreateIndexIfNotExist()
        {
            // Todo
        }

        private void MapProperties()
        {
            // Todo
        }
    }
}
