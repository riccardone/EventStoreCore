using System.Threading.Tasks;

namespace EventStore.Plugins.GeoReplica.Dispatcher.Http
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

        public async Task AppendAsynch(string stream, dynamic[] eventData)
        {
            await _connection.AppendToStreamAsync(stream, eventData);
        }

        public void Dispose()
        {
            // nothing to do here
        }
    }
}
