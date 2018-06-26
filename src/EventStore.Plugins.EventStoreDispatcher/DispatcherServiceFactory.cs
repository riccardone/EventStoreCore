using System;
using System.Collections.Generic;
using EventStore.ClientAPI;
using EventStore.Plugins.Dispatcher;
using EventStore.Plugins.EventStoreDispatcher.Config;
using EventStore.Plugins.EventStoreDispatcher.Http;
using EventStore.Plugins.EventStoreDispatcher.Tcp;

namespace EventStore.Plugins.EventStoreDispatcher
{
    public class DispatcherServiceFactory : IDispatcherServiceFactory
    {
        private static readonly Serilog.ILogger Log = Serilog.Log.ForContext<DispatcherServiceFactory>();
        private readonly Root _settings;

        public DispatcherServiceFactory(Root settings)
        {
            _settings = settings;
        }

        public IList<IDispatcherService> Create()
        {
            var results = new List<IDispatcherService>();
            if (_settings == null)
                return results;
            foreach (var destination in _settings.Destinations)
            {
                var origin = EventStoreConnection.Create(
                    new Uri(
                        $"tcp://{_settings.Origin.User}:{_settings.Origin.Password}@localhost:{_settings.Origin.LocalPort}"),
                    _settings.Origin.ToString());
                origin.ConnectAsync().Wait();
                var positionRepo = new PositionRepository("georeplica-position", "GeoPositionUpdated", origin);
                results.Add(new DispatcherService(origin, destination.InputStream, destination.Interval,
                    destination.BatchSize, BuildDispatcherToDestination(_settings.Origin, destination), positionRepo));
            }
            return results;
        }

        private static IDispatcher BuildDispatcherToDestination(Origin origin, Destination destination)
        {
            if (IsHttp(destination.ConnectionString.ToString()))
                return new HttpDispatcher(origin.ToString(), $"{destination.Name}-{destination.Id}", EventStoreHttpConnection.Create(Http.ConnectionSettings.Default, destination.ConnectionString));

            var connectionToDestination = EventStoreConnection.Create(destination.ConnectionString, destination.ToString());
            connectionToDestination.ConnectAsync().Wait();
            return new TcpDispatcher(origin.ToString(), destination.ToString(), connectionToDestination);
        }

        private static bool IsHttp(string connectionString)
        {
            return Uri.TryCreate(connectionString, UriKind.Absolute, out var uriResult)
                   && (uriResult.Scheme == Uri.UriSchemeHttp || uriResult.Scheme == Uri.UriSchemeHttps);
        }
    }
}
