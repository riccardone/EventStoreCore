using System;
using System.Collections;
using System.Collections.Concurrent;
using System.Collections.Generic;
using System.Text;
using System.Threading;
using System.Threading.Tasks;
using EventStore.ClientAPI;
using EventStore.Plugins.Dispatcher;
using Newtonsoft.Json;

namespace EventStore.Plugins.EventStoreDispatcher
{
    public class DispatcherServiceService : IDispatcherService
    {
        private static readonly Serilog.ILogger Log = Serilog.Log.ForContext<DispatcherServiceService>();
        private readonly IDictionary<string, IDispatcher> _dispatchers;
        private readonly IPositionRepository _positionRepository;
        private readonly IEventStoreConnection _local;
        private Position _lastPosition;
        private readonly CancellationToken _cancellationToken = new CancellationToken();
        private readonly ConcurrentDictionary<string, ConcurrentQueue<EventData>> _cache = new ConcurrentDictionary<string, ConcurrentQueue<EventData>>();
        private int _batchLimit = 500;
        private int _intervalInSeconds = 10;

        public DispatcherServiceService(IEventStoreConnection local,
            IDictionary<string, IDispatcher> dispatchers, IPositionRepository positionRepository)
        {
            _dispatchers = dispatchers;
            _positionRepository = positionRepository;
            _local = local;
            foreach (var dispatcher in _dispatchers)
            {
                _cache.TryAdd(dispatcher.Value.Destination, new ConcurrentQueue<EventData>());
            }
        }

        public async void Start()
        {
            _lastPosition = _positionRepository.GetAsynch().Result;
            _local.SubscribeToAllFrom(_lastPosition, CatchUpSubscriptionSettings.Default, EventAppeared);
            await PeriodicBulkAsync(TimeSpan.FromSeconds(_intervalInSeconds), _cancellationToken);
        }

        private Task EventAppeared(EventStoreCatchUpSubscription eventStoreCatchUpSubscription, ResolvedEvent resolvedEvent)
        {
            // Allow only user events and metadata events
            if (resolvedEvent.Event.EventType.StartsWith("$") && !resolvedEvent.Event.EventType.StartsWith("$$$") ||
                resolvedEvent.Event.EventType.Equals(_positionRepository.PositionEventType))
                return Task.CompletedTask;

            var metadata = DeserializeObject(resolvedEvent.Event.Metadata) ?? new Dictionary<string, string>();

            if (metadata.ContainsKey("$local"))
                return Task.CompletedTask;

            byte[] metadataAsBytes = null;

            foreach (var dispatcher in _dispatchers)
            {
                try
                {
                    // We don't want to replicate an event back to its origin
                    if (metadata.ContainsKey("$origin") && metadata["$origin"].Equals(dispatcher.Value.Destination))
                        continue;

                    metadataAsBytes = EnrichMetadata(resolvedEvent, metadata, dispatcher);
                    
                    _cache[dispatcher.Value.Destination].Enqueue(new EventData(resolvedEvent.Event.EventId,
                        resolvedEvent.Event.EventType, resolvedEvent.Event.IsJson, resolvedEvent.Event.Data,
                        metadataAsBytes));
                }
                catch (Exception e)
                {
                    Log.Error(e, "Error during geo-replication");
                }
            }

            return Task.CompletedTask;
        }

        private async Task PeriodicBulkAsync(TimeSpan interval, CancellationToken cancellationToken)
        {
            while (true)
            {
                await BulkIt();
                await Task.Delay(interval, cancellationToken);
            }
        }

        private async Task BulkIt()
        {
            foreach (var cacheKey in _cache.Keys)
            {
                var batch = new ArrayList();
                for (var i = 0; i < _cache[cacheKey].Count; i++)
                {
                    _cache[cacheKey].TryDequeue(out var result);
                    batch.Add(result);
                }
                await _dispatchers[cacheKey].BulkAppendAsynch("$ciccio", batch.ToArray());
                Log.Information($"Dispatched {batch.Count} Events to '{cacheKey}'");
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

            if (!metadata.ContainsKey("$eventStreamId"))
                metadata.Add("$eventStreamId", resolvedEvent.Event.EventStreamId);
            
            if (!metadata.ContainsKey("$position"))
                metadata.Add("$position", resolvedEvent.OriginalPosition.ToString());

            if (!metadata.ContainsKey("$eventNumber"))
                metadata.Add("$eventNumber", resolvedEvent.Event.EventNumber.ToString());

            if (!metadata.ContainsKey("$expectedVersion"))
                metadata.Add("$expectedVersion", (resolvedEvent.Event.EventNumber - 1).ToString());

            return SerializeObject(metadata);
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
