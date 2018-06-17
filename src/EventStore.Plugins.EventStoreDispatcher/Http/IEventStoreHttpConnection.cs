using System.Threading.Tasks;

namespace EventStore.Plugins.EventStoreDispatcher.Http
{
    public interface IEventStoreHttpConnection
    {
        Task AppendToStreamAsync(long expectedVersion, dynamic evt, byte[] metadata);
    }
}
