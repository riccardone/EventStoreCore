using System;
using System.Linq;
using System.Runtime.InteropServices;
using System.Threading.Tasks;
using EventStore.ClientAPI;
using Microsoft.Win32.SafeHandles;

namespace EventStore.Plugins.GeoReplica.Dispatcher.Tcp
{
    public class TcpDispatcher : IDispatcher
    {
        private static readonly Serilog.ILogger Log = Serilog.Log.ForContext<TcpDispatcher>();
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

        public async Task AppendAsynch(string stream, dynamic[] eventData)
        {
            await _connectionToDestination.AppendToStreamAsync(stream, -2, ToEventData(eventData));
        }

        private static EventData[] ToEventData(dynamic[] eventData)
        {
            return eventData.Cast<EventData>().ToArray();
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
                _connectionToDestination?.Close();
                _connectionToDestination?.Dispose();
            }

            _disposed = true;
        }
    }
}
