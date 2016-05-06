using System;
using System.Net;
using EventStore.Core.Data;
using EventStore.Core.Services.EventProfiler.Strategy;

namespace EventStore.Profilers.ElasticSearch
{
    public class ElasticSearchProfilerStrategy : IEventProfilerStrategy
    {
        public string Name { get { return "ElasticSearchProfiler"; } }
        public EventProfilerPushResult PushMessageToProfiler(string streamId, ResolvedEvent ev)
        {
            var urlReq = string.Format("http://localhost:9200/eventstore/{0}/{1}", ev.Event.EventType, ev.Event.EventId);
            using (var client = new WebClient())
                client.UploadString(urlReq, "PUT", ev.OriginalEvent.DebugDataView);
            return EventProfilerPushResult.Sent;
        }
    }
}
