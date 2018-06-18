using System.Threading.Tasks;
using EventStore.ClientAPI;

namespace EventStore.Plugins.EventStoreDispatcher
{
    public interface IPositionRepository
    {
        Position Get();
        Task SetAsynch(Position? position);
    }
}
