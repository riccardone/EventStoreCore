using System.Threading.Tasks;
using EventStore.ClientAPI;

namespace EventStore.Plugins.EventStoreReceiver
{
    public interface ICheckpointRepository
    {
        string EventType { get; }
        Task<long> GetAsynch();
        void Set(long checkpoint);
    }
}
