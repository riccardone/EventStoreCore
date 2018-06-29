using System;
using System.Linq;
using System.Threading.Tasks;
using EventStore.ClientAPI;
using EventStore.Plugins.Dispatcher;

namespace EventStore.Plugins.EventStoreDispatcher.Http
{
    public class HttpDispatcher : IDispatcher
    {
        public string Origin { get; }
        public string Destination { get; }
        private readonly IEventStoreHttpConnection _connection;

        public HttpDispatcher(string origin, string destination, IEventStoreHttpConnection connection)
        {
            Origin = origin;
            Destination = destination;
            _connection = connection;
        }

        public Task AppendAsynch(string stream, dynamic[] eventData)
        {
            // TODO implement append with batches (and remove the dispatch with only 1 event)
            throw new NotImplementedException();
        }

        private static EventData[] ToEventData(dynamic[] eventData)
        {
            return eventData.Cast<EventData>().ToArray();
        }

        public void Dispose()
        {
            // nothing to do here
        }
    }
}
