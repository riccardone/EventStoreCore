using System;
using EventStore.ClientAPI;
using EventStore.Plugins.EventStoreReceiver.Config;
using EventStore.Plugins.Receiver;

namespace EventStore.Plugins.EventStoreReceiver
{
    public class ReceiverServiceFactory : IReceiverServiceFactory
    {
        private readonly Root _settings;

        public ReceiverServiceFactory(Root settings)
        {
            _settings = settings;
        }

        public string Name { get; }

        public IReceiverService Create()
        {
            var origin = EventStoreConnection.Create(
                new Uri(
                    $"tcp://{_settings.Receiver.User}:{_settings.Receiver.Password}@localhost:{_settings.Receiver.LocalPort}"),
                _settings.Receiver.ToString());
            origin.ConnectAsync().Wait();
            return new ReceiverService(_settings, origin);
        }
    }
}
