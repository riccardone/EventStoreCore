using System;
using System.Collections.Generic;
using System.Linq;
using System.Text;
using EventStore.Common.Log;
using EventStore.Core.Bus;
using EventStore.Core.Data;
using EventStore.Core.Messages;
using Newtonsoft.Json;

namespace EventStore.Core.Services.GeoReplica
{
    public class GeoReplicaService : IHandle<SystemMessage.BecomeMaster>, IHandle<StorageMessage.EventCommitted>
    {
        private readonly string _originMetadataKey;
        //private readonly string _positionStreamName;
        private readonly string _positionEventType;
        private readonly string _conflictDetectedEventType;
        private readonly string _sourceGuid;
        private static readonly ILogger Log = LogManager.GetLoggerFor<GeoReplicaService>();
        //private bool _started;
        private IGeoReplicaFactory[] _geoReplicaFactories;
        private Dictionary<string, IGeoReplica> _destinations; // 1. DestinationGuid, 2. Http or Tcp dispatcher

        public GeoReplicaService(IGeoReplicaFactory[] geoReplicaFactories)
        {
            _originMetadataKey = "origin";
            //_positionStreamName = "georeplica-position";
            _positionEventType = "GeoPositionUpdated";
            _conflictDetectedEventType = "ConflictDetected";
            _sourceGuid = "eeccf5ce-4f54-409d-8870-b35dd836cca6"; // TODO load from config
            _geoReplicaFactories = geoReplicaFactories;
            LoadConfiguration(Start);
        }

        private void Start()
        {
            foreach (var geoReplicaFactory in _geoReplicaFactories)
            {
                var element = geoReplicaFactory.Create();
                _destinations.Add(element.Key, element.Value);
            }
            //_started = true;
        }

        public void Handle(SystemMessage.BecomeMaster message)
        {
            Log.Debug("GeoReplicaService Became Master. It's time to send committed messages to the destinations.");
            InitOrEmpty();
            
        }

        private void InitOrEmpty()
        {
            _destinations = new Dictionary<string, IGeoReplica>();
        }

        private void LoadConfiguration(Action continueWith)
        {
            // TODO
            //_geoReplicaFactory = new FakeGeoReplicaFactory();

            continueWith();
        }

        public void Handle(StorageMessage.EventCommitted message)
        {
            try
            {
                // Allow only user events and metadata events
                if (message.Event.EventType.StartsWith("$") && !message.Event.EventType.StartsWith("$$$") ||
                    message.Event.EventType.Equals(_positionEventType) ||
                    message.Event.EventType.Equals(_conflictDetectedEventType))
                    return;

                // Check if we can replicate the event to the configured destination
                var metadata = DeserializeObject(message.Event.Metadata);

                // TODO _destinations.AsParallel()... ?

                foreach (var destination in _destinations)
                {
                    if (metadata.ContainsKey(_originMetadataKey) && metadata[_originMetadataKey].Equals(destination.Key))
                        return;

                    // Enrich the metadata with the source guid
                    metadata.Add(_originMetadataKey, _sourceGuid);
                    var metadataAsBytes = SerializeObject(metadata);

                    try
                    {
                        // TODO
                        //await destination.Value.DispatchAsynch(message.Event.EventNumber - 1, message,
                        //    metadataAsBytes);
                        //await _positionRepository.SetAsynch(message.Event.EventNumber);
                    }
                    catch (Exception wev)
                    {
                        Log.Warn(wev.Message);
                        //await _source.AppendToStreamAsync($"$conflicts-{destination.Key}", ExpectedVersion.Any,
                        //    new EventData(Guid.NewGuid(), _conflictDetectedEventType, true, SerializeObject(
                        //        new Dictionary<string, string>
                        //        {
                        //            {"Error", wev.GetBaseException().Message},
                        //            {"OriginalEvent", Encoding.UTF8.GetString(SerializeObject(resolvedEvent))}
                        //        }), null));
                        //await destination.Value.DispatchAsynch(-2, resolvedEvent, metadataAsBytes);
                    }
                }
            }
            catch (Exception e)
            {
                Log.ErrorException(e, "Error during replication");
            }
        }

        private static IDictionary<string, string> DeserializeObject(byte[] obj)
        {
            return JsonConvert.DeserializeObject<Dictionary<string, string>>(
                Encoding.UTF8.GetString(obj));
        }

        private static byte[] SerializeObject(object obj)
        {
            var jsonObj = JsonConvert.SerializeObject(obj);
            var data = Encoding.UTF8.GetBytes(jsonObj);
            return data;
        }
    }
}
