using System.Threading.Tasks;

namespace EventStore.Plugins.GeoReplica.Receiver
{
    public interface ICheckpointRepository
    {
        string EventType { get; }
        Task<long> GetAsynch();
        void Set(long checkpoint);
    }
}
