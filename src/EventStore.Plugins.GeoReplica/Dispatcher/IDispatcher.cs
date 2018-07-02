using System;
using System.Threading.Tasks;

namespace EventStore.Plugins.GeoReplica.Dispatcher
{
    public interface IDispatcher : IDisposable
    {
        string Origin { get; }
        string Destination { get; }
        Task AppendAsynch(string stream, dynamic[] eventDatas);
    }
}
