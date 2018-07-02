using System;
using System.Collections.Generic;
using System.Threading.Tasks;
using EventStore.ClientAPI;
using EventStore.Plugins.GeoReplica.Config;
using EventStore.Plugins.GeoReplica.Dispatcher;
using EventStore.Plugins.GeoReplica.Dispatcher.Http;
using EventStore.Plugins.GeoReplica.Dispatcher.Tcp;
using EventStore.Plugins.GeoReplica.Receiver;
using ConnectionSettingsBuilder = EventStore.Plugins.GeoReplica.Dispatcher.Http.ConnectionSettingsBuilder;

namespace EventStore.Plugins.GeoReplica
{
    public class GeoReplicaServiceFactory : IEventStoreServiceFactory
    {
        private static readonly Serilog.ILogger Log = Serilog.Log.ForContext<GeoReplicaServiceFactory>();
        private readonly Root _settings;

        public GeoReplicaServiceFactory(Root settings)
        {
            _settings = settings;
        }

        public IList<IEventStoreService> Create()
        {
            if (_settings == null)
                return new List<IEventStoreService>();
            var services = new List<IEventStoreService>();
            BuildDestinations(services);
            BuildReceivers(services);
            return services;
        }

        private void BuildReceivers(ICollection<IEventStoreService> services)
        {
            foreach (var receiver in _settings.Receivers)
            {
                var local = EventStoreConnection.Create(
                    new Uri(
                        $"tcp://{receiver.User}:{receiver.Password}@localhost:{_settings.LocalInstance.LocalPort}"),
                    $"local-{_settings.LocalInstance}");
                local.ConnectAsync().Wait();
                services.Add(new ReceiverService(receiver, local,
                    new CheckpointRepository($"georeplica-checkpoint-{receiver}", "GeoPositionUpdated", local,
                        receiver.CheckpointInterval)));
            }
        }

        private void BuildDestinations(ICollection<IEventStoreService> services)
        {
            foreach (var destination in _settings.Destinations)
            {
                var origin = EventStoreConnection.Create(
                    new Uri(
                        $"tcp://{_settings.LocalInstance.User}:{_settings.LocalInstance.Password}@localhost:{_settings.LocalInstance.LocalPort}"),
                    $"local-{_settings.LocalInstance}");
                origin.ConnectAsync().Wait();
                Log.Information($"Connected to '{destination}'");
                services.Add(new DispatcherService(origin, destination.InputStream, destination.Interval,
                    destination.BatchSize, BuildDispatcherForDestination(_settings.LocalInstance, destination).Result,
                    new PositionRepository($"georeplica-position-{destination}", "GeoPositionUpdated", origin)));
            }
        }

        private static async Task<IDispatcher> BuildDispatcherForDestination(LocalInstance origin, Destination destination)
        {
            if (IsHttp(destination.ConnectionString.ToString()))
            {
                var settings =
                    new ConnectionSettingsBuilder().SetDefaultUserCredentials(new UserCredentials(destination.User,
                        destination.Password));
                return new HttpDispatcher(origin.ToString(), $"{destination.Name}-{destination.Id}",
                    EventStoreHttpConnection.Create(settings, destination.ConnectionString));
            }

            var connectionToDestination = EventStoreConnection.Create(destination.ConnectionString, origin.ToString());
            await connectionToDestination.ConnectAsync();
            return new TcpDispatcher(origin.ToString(), destination.ToString(), connectionToDestination);
        }

        private static bool IsHttp(string connectionString)
        {
            return Uri.TryCreate(connectionString, UriKind.Absolute, out var uriResult)
                   && (uriResult.Scheme == Uri.UriSchemeHttp || uriResult.Scheme == Uri.UriSchemeHttps);
        }
    }
}
