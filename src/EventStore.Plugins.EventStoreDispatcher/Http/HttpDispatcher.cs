using System.Threading.Tasks;
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

        public async Task DispatchAsynch(long version, dynamic evt, byte[] metadata)
        {
            await _connection.AppendToStreamAsync(version, evt, metadata);
        }

        public void Dispose()
        {
            // nothing to do here
        }
    }
}
