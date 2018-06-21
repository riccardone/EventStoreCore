using System;
using System.Threading.Tasks;

namespace EventStore.Plugins.EventStoreDispatcher
{
    public interface IDispatcher : IDisposable
    {
        string Origin { get; }
        string Destination { get; }
        Task DispatchAsynch(long version, dynamic resolvedEvent, byte[] metadata);
        Task BulkAppendAsynch(string stream, dynamic[] eventData);
    }
}
