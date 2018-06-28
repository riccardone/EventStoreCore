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
            if (_settings == null)
                return new List<IDispatcherService>();
            var dispatchers = new List<IDispatcherService>();
            foreach (var destination in _settings.Destinations)
            {
                var origin = EventStoreConnection.Create(
                    new Uri(
                        $"tcp://{_settings.Origin.User}:{_settings.Origin.Password}@localhost:{_settings.Origin.LocalPort}"),
                    $"local-{_settings.Origin}");
                origin.ConnectAsync().Wait();
                Log.Information($"Connected to '{destination}'");
                dispatchers.Add(new DispatcherService(origin, destination.InputStream, destination.Interval,
                    destination.BatchSize, BuildDispatcherForDestination(_settings.Origin, destination),
                    new PositionRepository("georeplica-position", "GeoPositionUpdated", origin)));
            }
            return dispatchers;
        }

        private static IDispatcher BuildDispatcherForDestination(Origin origin, Destination destination)
        {
            if (IsHttp(destination.ConnectionString.ToString()))
                return new HttpDispatcher(origin.ToString(), $"{destination.Name}-{destination.Id}", EventStoreHttpConnection.Create(Http.ConnectionSettings.Default, destination.ConnectionString));

            var connectionToDestination = EventStoreConnection.Create(destination.ConnectionString, origin.ToString());
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
