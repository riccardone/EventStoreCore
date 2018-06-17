using System;
using System.Threading.Tasks;

namespace EventStore.Plugins.Dispatcher
{
    public interface IDispatcher : IDisposable
    {
        string Origin { get; }
        string Destination { get; }
        Task DispatchAsynch(long version, dynamic resolvedEvent, byte[] metadata);
    }
}
