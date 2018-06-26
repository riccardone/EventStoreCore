using System;
using EventStore.ClientAPI;
using EventStore.Plugins.EventStoreDispatcher.Config;
using EventStore.Plugins.EventStoreDispatcher.Http;
using EventStore.Plugins.EventStoreDispatcher.Tcp;

namespace EventStore.Plugins.EventStoreDispatcher
{
    public class DispatcherFactory : IDispatcherFactory
    {
        private readonly Root _settings;

        public string Name => "EventStore Dispatcher Factory";

        public DispatcherFactory(Root settings)
        {
            _settings = settings;
        }

        public IDispatcher Create(Destination destination)
        {
            var dispatcher = BuildDispatcherToDestination(_settings.Origin, destination);
            return dispatcher;
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
