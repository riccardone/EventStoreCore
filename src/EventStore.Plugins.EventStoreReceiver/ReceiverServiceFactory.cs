using System;
using System.Collections.Generic;
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

        public string Name => $"{_settings.Local.Name}";

        public IList<IReceiverService> Create()
        {
            if (_settings == null)
                return null;
            var receivers = new List<IReceiverService>();
            foreach (var receiver in _settings.Receivers)
            {
                var local = EventStoreConnection.Create(
                    new Uri(
                        $"tcp://{receiver.User}:{receiver.Password}@localhost:{_settings.Local.Port}"),
                    $"local-{_settings.Local}");
                local.ConnectAsync().Wait();
                receivers.Add(new ReceiverService(receiver, local,
                    new CheckpointRepository($"georeplica-checkpoint-{receiver}", "GeoPositionUpdated", local,
                        receiver.CheckpointInterval)));
            }
            return receivers;
        }
    }
}
