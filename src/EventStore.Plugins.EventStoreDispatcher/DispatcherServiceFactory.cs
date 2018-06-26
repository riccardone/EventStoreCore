using System;
using System.Collections.Generic;
using EventStore.ClientAPI;
using EventStore.Plugins.Dispatcher;
using EventStore.Plugins.EventStoreDispatcher.Config;

namespace EventStore.Plugins.EventStoreDispatcher
{
    public class DispatcherServiceFactory : IDispatcherServiceFactory
    {
        private readonly Root _settings;
        private readonly IDispatcherFactory _dispatcherFactory;

        public DispatcherServiceFactory(Root settings, IDispatcherFactory dispatcherFactory)
        {
            _settings = settings;
            _dispatcherFactory = dispatcherFactory;
        }

        public IList<IDispatcherService> Create()
        {
            var results = new List<IDispatcherService>();
            foreach (var destination in _settings.Destinations)
            {
                var origin = EventStoreConnection.Create(
                    new Uri(
                        $"tcp://{_settings.Origin.User}:{_settings.Origin.Password}@localhost:{_settings.Origin.LocalPort}"),
                    _settings.Origin.ToString());
                origin.ConnectAsync().Wait();
                var positionRepo = new PositionRepository("georeplica-position", "GeoPositionUpdated", origin);
                results.Add(new DispatcherService(origin, destination.InputStream, 3600, 5000, _dispatcherFactory.Create(destination), positionRepo));
            }
            return results;
        }
    }
}
