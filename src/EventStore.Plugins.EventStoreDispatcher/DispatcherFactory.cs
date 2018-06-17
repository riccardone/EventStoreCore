using System;
using System.Collections.Generic;
using EventStore.ClientAPI;
using EventStore.Plugins.Dispatcher;
using EventStore.Plugins.EventStoreDispatcher.Config;
using EventStore.Plugins.EventStoreDispatcher.Http;
using EventStore.Plugins.EventStoreDispatcher.Tcp;

namespace EventStore.Plugins.EventStoreDispatcher
{
    public class DispatcherFactory : IDispatcherFactory
    {
        private readonly IConfigProvider _configProvider;

        public string Name => "EventStore Dispatcher Factory";

        public DispatcherFactory(IConfigProvider configProvider)
        {
            _configProvider = configProvider;
        }

        public IDictionary<string, IDispatcher> Create()
        {
            var results = new Dictionary<string, IDispatcher>();
            var settings = _configProvider.GetSettings();
            foreach (var setting in settings.Destinations)
            {
                var dispatcher = BuildDispatcherToDestination(settings.Origin, setting);
                results.Add(setting.Name, dispatcher);
            }
            return results;
        }

        private static IDispatcher BuildDispatcherToDestination(Origin origin, Destination destination)
        {
            if (IsHttp(destination.ConnectionString.ToString()))
                return new HttpDispatcher(origin.ToString(), $"{destination.Name}-{destination.Id}", EventStoreHttpConnection.Create(Http.ConnectionSettings.Default, destination.ConnectionString));
            var connection = EventStoreConnection.Create(destination.ConnectionString, destination.ToString());
            connection.ConnectAsync().Wait();
            return new TcpDispatcher(origin.ToString(), destination.ToString(), connection);
        }

        private static bool IsHttp(string connectionString)
        {
            return Uri.TryCreate(connectionString, UriKind.Absolute, out var uriResult)
                   && (uriResult.Scheme == Uri.UriSchemeHttp || uriResult.Scheme == Uri.UriSchemeHttps);
        }
    }
}
