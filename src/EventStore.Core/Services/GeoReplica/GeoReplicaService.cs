using System;
using System.Collections.Generic;
using System.Text;
using EventStore.Common.Log;
using EventStore.Core.Bus;
using EventStore.Core.Data;
using EventStore.Core.Messages;
using EventStore.Plugins.Dispatcher;
using Newtonsoft.Json;

namespace EventStore.Core.Services.GeoReplica
{
    public class GeoReplicaService :
        IHandle<SystemMessage.BecomeMaster>,
        IHandle<StorageMessage.EventCommitted>,
        IHandle<SystemMessage.StateChangeMessage>
    {
        private bool _started;
        private readonly string _originMetadataKey;
        //private readonly string _positionStreamName;
        private readonly string _positionEventType;
        private readonly string _conflictDetectedEventType;
        private static readonly ILogger Log = LogManager.GetLoggerFor<GeoReplicaService>();
        private readonly IDispatcherFactory[] _dispatcherFactories;
        private IDictionary<string, IDispatcher> _dispatchers; // 1. name-id, 2. Http or Tcp dispatcher

        public GeoReplicaService(IDispatcherFactory[] dispatcherFactories)
        {
            _originMetadataKey = "origin";
            //_positionStreamName = "georeplica-position";
            _positionEventType = "GeoPositionUpdated";
            _conflictDetectedEventType = "ConflictDetected";
            _dispatcherFactories = dispatcherFactories;
        }

        private void Start()
        {
            _dispatchers = new Dictionary<string, IDispatcher>();
            foreach (var dispatcherFactory in _dispatcherFactories)
            {
                var dispatchers = dispatcherFactory.Create();
                foreach (var key in dispatchers.Keys)
                    if (!_dispatchers.ContainsKey(key))
                        _dispatchers.Add(key, dispatchers[key]);
            }
            _started = true;
        }

        private void Stop()
        {
            foreach (var dispatcher in _dispatchers)
                dispatcher.Value.Dispose();
            _started = false;
        }

        public void Handle(SystemMessage.BecomeMaster message)
        {
            Log.Debug("GeoReplicaService Became Master. It's time to send committed messages to the destinations.");
            LoadConfiguration(Start);
        }

        private static void LoadConfiguration(Action continueWith)
        {
            continueWith();
        }

        public async void Handle(StorageMessage.EventCommitted message)
        {
            // Allow only user events and metadata events
            if (!_started || message.Event.EventType.StartsWith("$") && !message.Event.EventType.StartsWith("$$$") ||
                message.Event.EventType.Equals(_positionEventType) ||
                message.Event.EventType.Equals(_conflictDetectedEventType))
                return;
            try
            {
                var metadata = DeserializeObject(message.Event.Metadata) ?? new Dictionary<string, string>();

                // TODO use some degree of parallelism to run the following?
                foreach (var dispatcher in _dispatchers)
                {
                    // We don't want to replicate an event back to its origin
                    if (metadata.ContainsKey(_originMetadataKey) && metadata[_originMetadataKey].Equals(dispatcher.Value.Destination))
                        continue;

                    // Enrich the metadata with the origin
                    if (metadata.ContainsKey(_originMetadataKey))
                        metadata[_originMetadataKey] = dispatcher.Value.Origin;
                    else
                        metadata.Add(_originMetadataKey, dispatcher.Value.Origin);
                    var metadataAsBytes = SerializeObject(metadata);

                    try
                    {
                        await dispatcher.Value.DispatchAsynch(message.Event.EventNumber - 1, message, metadataAsBytes);
                        Log.Info($"Event '{message.Event.EventId}' dispatched to '{dispatcher.Value.Destination}'");
                        // TODO how we save the replica position from within EventStore? 
                        //await _positionRepository.SetAsynch(message.Event.EventNumber);
                    }
                    catch (Exception ex)
                    {
                        Log.Warn("Exception thrown during replication", ex);
                        // TODO how we write a new $conflicts... stream from within EventStore?
                        //await _source.AppendToStreamAsync($"$conflicts-{dispatcher.Origin}-{dispatcher.Destination}", ExpectedVersion.Any,
                        //    new EventData(Guid.NewGuid(), _conflictDetectedEventType, true, SerializeObject(
                        //        new Dictionary<string, string>
                        //        {
                        //            {"Error", wev.GetBaseException().Message},
                        //            {"OriginalEvent", Encoding.UTF8.GetString(SerializeObject(resolvedEvent))}
                        //        }), null));
                        await dispatcher.Value.DispatchAsynch(-2, message, metadataAsBytes);
                    }
                }
            }
            catch (Exception e)
            {
                Log.ErrorException(e, "Error during geo-replication");
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

        public void Handle(SystemMessage.StateChangeMessage message)
        {
            if (message.State == VNodeState.Master)
                return;
            Stop();
        }
    }
}
