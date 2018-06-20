using System;
using System.Collections.Generic;
using System.Text;
using System.Threading.Tasks;
using EventStore.ClientAPI;
using EventStore.ClientAPI.Exceptions;
using EventStore.Plugins.Dispatcher;
using Newtonsoft.Json;

namespace EventStore.Plugins.EventStoreDispatcher
{
    public class SubscriberService : ISubscriberService
    {
        private static readonly Serilog.ILogger Log = Serilog.Log.ForContext<SubscriberService>();
        private readonly IDictionary<string, IDispatcher> _dispatchers;
        private readonly IPositionRepository _positionRepository;
        private readonly IEventStoreConnection _local;
        private Position _lastPosition;

        public SubscriberService(IEventStoreConnection local,
            IDictionary<string, IDispatcher> dispatchers, IPositionRepository positionRepository)
        {
            _dispatchers = dispatchers;
            _positionRepository = positionRepository;
            _local = local;
        }

        public void Start()
        {
            _lastPosition = _positionRepository.GetAsynch().Result;
            _local.SubscribeToAllFrom(_lastPosition, CatchUpSubscriptionSettings.Default, EventAppeared);
        }

        private async Task EventAppeared(EventStoreCatchUpSubscription eventStoreCatchUpSubscription, ResolvedEvent resolvedEvent)
        {
            // Allow only user events and metadata events
            if (resolvedEvent.Event.EventType.StartsWith("$") && !resolvedEvent.Event.EventType.StartsWith("$$$") ||
                resolvedEvent.Event.EventType.Equals(_positionRepository.PositionEventType))
                return;

            var metadata = DeserializeObject(resolvedEvent.Event.Metadata) ?? new Dictionary<string, string>();
            byte[] metadataAsBytes = null;
            
            foreach (var dispatcher in _dispatchers)
            {
                try
                {
                    // We don't want to replicate an event back to its origin
                    if (metadata.ContainsKey("$origin") && metadata["$origin"].Equals(dispatcher.Value.Destination))
                        continue;
                    
                    metadataAsBytes = EnrichMetadata(resolvedEvent, metadata, dispatcher);

                    await dispatcher.Value.DispatchAsynch(resolvedEvent.Event.EventNumber - 1, resolvedEvent, metadataAsBytes);
                    Log.Information($"Event '{resolvedEvent.Event.EventId}' dispatched to '{dispatcher.Value.Destination}'");
                }
                catch (WrongExpectedVersionException ex)
                {
                    await HandleConflict(resolvedEvent, dispatcher, ex, metadataAsBytes ?? SerializeObject(metadata));
                    Log.Warning("WrongExpectedVersionException thrown during replication: {0}", ex);
                }
                catch (Exception e)
                {
                    Log.Error(e, "Error during geo-replication");
                }
            }
        }

        private static byte[] EnrichMetadata(ResolvedEvent resolvedEvent, IDictionary<string, string> metadata, KeyValuePair<string, IDispatcher> dispatcher)
        {
            if (metadata.ContainsKey("$origin"))
                metadata["$origin"] = dispatcher.Value.Origin;
            else
                metadata.Add("$origin", dispatcher.Value.Origin);

            if (!metadata.ContainsKey("$applies"))
                metadata.Add("$applies", resolvedEvent.Event.Created.ToString("o"));

            if (!metadata.ContainsKey("$originalEventNumber"))
                metadata.Add("$originalEventNumber", resolvedEvent.OriginalEventNumber.ToString());

            return SerializeObject(metadata);
        }

        private async Task HandleConflict(ResolvedEvent resolvedEvent, KeyValuePair<string, IDispatcher> dispatcher,
            WrongExpectedVersionException ex, byte[] metadataAsBytes)
        {
            var conflictStreamName = $"$conflicts-{dispatcher.Value.Origin}-{dispatcher.Value.Destination}";
            await _local.AppendToStreamAsync(conflictStreamName, ExpectedVersion.Any,
                new EventData(resolvedEvent.Event.EventId, resolvedEvent.Event.EventType, resolvedEvent.Event.IsJson,
                    resolvedEvent.Event.Data, metadataAsBytes));
            await dispatcher.Value.DispatchAsynch(-2, resolvedEvent, metadataAsBytes);
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
