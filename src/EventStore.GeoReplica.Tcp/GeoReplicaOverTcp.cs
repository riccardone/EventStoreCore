using System.Threading.Tasks;
using EventStore.ClientAPI;
using EventStore.Core.Services.GeoReplica;

namespace EventStore.GeoReplica.Tcp
{
    public class GeoReplicaOverTcp : IGeoReplica
    {
        private readonly IEventStoreConnection _connection;
        public string Name { get; }

        public GeoReplicaOverTcp(string name, IEventStoreConnection connection)
        {
            Name = name;
            _connection = connection;
        }

        public async Task DispatchAsynch(long version, Core.Data.ResolvedEvent evt, byte[] metadata)
        {
            await _connection.AppendToStreamAsync(evt.OriginalStreamId, version,
                new EventData(evt.Event.EventId, evt.Event.EventType, evt.OriginalEvent.IsJson, evt.Event.Data,
                    metadata));
        }
    }
}
