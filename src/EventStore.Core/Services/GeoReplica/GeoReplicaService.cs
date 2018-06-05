using System;
using System.Collections.Generic;
using EventStore.Common.Log;
using EventStore.Core.Bus;
using EventStore.Core.Messages;

namespace EventStore.Core.Services.GeoReplica
{
    public class GeoReplicaService : IHandle<SystemMessage.BecomeMaster>, IHandle<StorageMessage.EventCommitted>
    {
        private static readonly ILogger Log = LogManager.GetLoggerFor<GeoReplicaService>();
        private bool _started;
        private IGeoReplicaFactory _geoReplicaFactory;
        private Dictionary<string, IGeoReplica> _destinations;

        private void Start()
        {
            _destinations = _geoReplicaFactory.Create();
            _started = true;
        }

        public void Handle(SystemMessage.BecomeMaster message)
        {
            Log.Debug("GeoReplicaService Became Master. It's time to send committed messages to the destinations.");
            InitOrEmpty();
            LoadConfiguration(Start);
        }

        private void InitOrEmpty()
        {
            _destinations = new Dictionary<string, IGeoReplica>();
        }

        private void LoadConfiguration(Action continueWith)
        {
            // TODO
            _geoReplicaFactory = new FakeGeoReplicaFactory();

            continueWith();
        }

        public void Handle(StorageMessage.EventCommitted message)
        {
            if (!_started || message.Event.EventType.StartsWith("$") && !message.Event.EventType.StartsWith("$$$")) // Add more filters
                return;

            // TODO enrich metadata with origin

            // TODO
            // _destinations.AsParallel()...
        }
    }
}
