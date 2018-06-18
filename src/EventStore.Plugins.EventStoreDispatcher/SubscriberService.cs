using System;
using System.Collections.Generic;
using System.Text;
using System.Threading.Tasks;
using EventStore.ClientAPI;
using EventStore.Plugins.Dispatcher;
using Newtonsoft.Json;

namespace EventStore.Plugins.EventStoreDispatcher
{
    public class SubscriberService : ISubscriberService
    {
        private readonly string _originMetadataKey;
        private readonly string _positionStreamName;
        private readonly string _positionEventType;
        private readonly string _conflictDetectedEventType;
        private readonly IEventStoreConnection _source;
        private readonly IDictionary<string, IDispatcher> _dispatchers;
        private readonly IPositionRepository _positionRepository;
        private Position _lastPosition;

        public SubscriberService(string originMetadataKey, string positionStreamName, string positionEventType, string conflictDetectedEventType, IEventStoreConnection source, IDictionary<string, IDispatcher> dispatchers, IPositionRepository positionRepository)
        {
            _originMetadataKey = originMetadataKey;
            _positionStreamName = positionStreamName;
            _positionEventType = positionEventType;
            _conflictDetectedEventType = conflictDetectedEventType;
            _source = source;
            _dispatchers = dispatchers;
            _positionRepository = positionRepository;
        }
        
        public void Start()
        {
            _lastPosition = Position.Start; // _positionRepository.Get();
            // TODO set readBatchSize to 1000
            _source.SubscribeToAllFrom(_lastPosition, CatchUpSubscriptionSettings.Default, EventAppeared);
        }

        private async Task EventAppeared(EventStoreCatchUpSubscription eventStoreCatchUpSubscription, ResolvedEvent resolvedEvent)
        {
            // Allow only user events and metadata events
            if (resolvedEvent.Event.EventType.StartsWith("$") && !resolvedEvent.Event.EventType.StartsWith("$$$") ||
                resolvedEvent.Event.EventType.Equals(_positionEventType) ||
                resolvedEvent.Event.EventType.Equals(_conflictDetectedEventType))
                return;
            try
            {
                var metadata = DeserializeObject(resolvedEvent.Event.Metadata) ?? new Dictionary<string, string>();

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
                        await dispatcher.Value.DispatchAsynch(resolvedEvent.Event.EventNumber - 1, resolvedEvent, metadataAsBytes);
                        //Log.Info($"Event '{resolvedEvent.Event.EventId}' dispatched to '{dispatcher.Value.Destination}'");
                        // TODO how we save the replica position from within EventStore? 
                        //await _positionRepository.SetAsynch(message.Event.EventNumber);
                    }
                    catch (Exception ex)
                    {
                        //Log.Warn("Exception thrown during replication", ex);
                        // TODO how we write a new $conflicts... stream from within EventStore?
                        //await _source.AppendToStreamAsync($"$conflicts-{dispatcher.Origin}-{dispatcher.Destination}", ExpectedVersion.Any,
                        //    new EventData(Guid.NewGuid(), _conflictDetectedEventType, true, SerializeObject(
                        //        new Dictionary<string, string>
                        //        {
                        //            {"Error", wev.GetBaseException().Message},
                        //            {"OriginalEvent", Encoding.UTF8.GetString(SerializeObject(resolvedEvent))}
                        //        }), null));
                        await dispatcher.Value.DispatchAsynch(-2, resolvedEvent, metadataAsBytes);
                    }
                }
            }
            catch (Exception e)
            {
                //Log.ErrorException(e, "Error during geo-replication");
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
