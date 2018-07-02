using System.Threading.Tasks;

namespace EventStore.Plugins.GeoReplica.Dispatcher.Http
{
    public interface IEventStoreHttpConnection
    {
        //Task AppendToStreamAsync(long expectedVersion, dynamic evt, byte[] metadata);
        Task AppendToStreamAsync(string stream, dynamic[] events);
    }
}
