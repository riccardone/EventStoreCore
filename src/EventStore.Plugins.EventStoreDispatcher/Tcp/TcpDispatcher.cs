using System;
using System.Runtime.InteropServices;
using System.Threading.Tasks;
using EventStore.ClientAPI;
using EventStore.Plugins.Dispatcher;
using Microsoft.Win32.SafeHandles;

namespace EventStore.Plugins.EventStoreDispatcher.Tcp
{
    public class TcpDispatcher : IDispatcher
    {
        private readonly IEventStoreConnection _connectionToDestination;
        public string Origin { get; }
        public string Destination { get; }
        private bool _disposed;
        private readonly SafeHandle _handle = new SafeFileHandle(IntPtr.Zero, true);

        public TcpDispatcher(string origin, string destination, IEventStoreConnection connectionToDestination)
        {
            Origin = origin;
            Destination = destination;
            _connectionToDestination = connectionToDestination;
        }

        public async Task DispatchAsynch(long version, dynamic evt, byte[] metadata)
        {
            await _connectionToDestination.AppendToStreamAsync(evt.Event.EventStreamId, version,
                new EventData(evt.Event.EventId, evt.Event.EventType, evt.Event.IsJson, evt.Event.Data,
                    metadata));
        }

        public void Dispose()
        {
            Dispose(true);
            GC.SuppressFinalize(this);
        }

        // Protected implementation of Dispose pattern.
        protected virtual void Dispose(bool disposing)
        {
            if (_disposed)
                return;

            if (disposing)
            {
                _handle.Dispose();
                _connectionToDestination?.Dispose();
            }
            _disposed = true;
        }
    }
}
