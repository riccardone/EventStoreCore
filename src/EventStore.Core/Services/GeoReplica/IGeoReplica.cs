using System.Threading.Tasks;
using EventStore.Core.Data;

namespace EventStore.Core.Services.GeoReplica
{
    public interface IGeoReplica
    {
        string Name { get; }
        Task DispatchAsynch(long version, ResolvedEvent evt, byte[] metadata);
    }
}
