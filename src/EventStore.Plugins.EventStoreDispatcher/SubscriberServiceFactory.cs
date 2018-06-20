using System;
using EventStore.ClientAPI;
using EventStore.Plugins.Dispatcher;
using EventStore.Plugins.EventStoreDispatcher.Config;

namespace EventStore.Plugins.EventStoreDispatcher
{
    public class SubscriberServiceFactory : ISubscriberServiceFactory
    {
        private readonly Root _settings;
        private readonly IDispatcherFactory _dispatcherFactory;

        public SubscriberServiceFactory(Root settings, IDispatcherFactory dispatcherFactory)
        {
            _settings = settings;
            _dispatcherFactory = dispatcherFactory;
        }

        public ISubscriberService Create()
        {
            var origin = EventStoreConnection.Create(
                new Uri(
                    $"tcp://{_settings.Origin.User}:{_settings.Origin.Password}@localhost:{_settings.Origin.LocalPort}"),
                _settings.Origin.ToString());
            origin.ConnectAsync().Wait();
            var positionRepo = new PositionRepository("georeplica-position", "GeoPositionUpdated", origin);
            return new SubscriberService(origin, _dispatcherFactory.Create(), positionRepo);
        }
    }
}
