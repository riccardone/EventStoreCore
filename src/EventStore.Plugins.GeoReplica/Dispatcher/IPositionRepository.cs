using System.Threading.Tasks;
using EventStore.ClientAPI;

namespace EventStore.Plugins.GeoReplica.Dispatcher
{
    public interface IPositionRepository
    {
        string PositionEventType { get; }
        Task<Position> GetAsynch();
        Task SetAsynch(Position position);
    }
}
