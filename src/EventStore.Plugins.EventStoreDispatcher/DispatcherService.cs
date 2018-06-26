using System;
using System.Collections;
using System.Collections.Concurrent;
using System.Collections.Generic;
using System.Text;
using System.Threading.Tasks;
using System.Timers;
using System.Linq;
using EventStore.ClientAPI;
using EventStore.Plugins.Dispatcher;
using Newtonsoft.Json;

namespace EventStore.Plugins.EventStoreDispatcher
{
    public class DispatcherService : IDispatcherService
    {
        private static readonly Serilog.ILogger Log = Serilog.Log.ForContext<DispatcherService>();
        private readonly IDispatcher _dispatcher;
        private readonly IPositionRepository _positionRepository;
        private readonly IEventStoreConnection _local;
        private readonly string _ingestionStreamName;
        private readonly int _interval;
        private readonly int _batchSize;
        private Position _lastPosition;
        private readonly ConcurrentQueue<EventData> _cache = new ConcurrentQueue<EventData>();
        private static Timer _timer;

        public DispatcherService(IEventStoreConnection local, string ingestionStreamName, int interval, int batchSize,
            IDispatcher dispatcher, IPositionRepository positionRepository)
        {
            _dispatcher = dispatcher;
            _positionRepository = positionRepository;
            _local = local;
            _ingestionStreamName = ingestionStreamName;
            _interval = interval;
            _batchSize = batchSize;
        }

        public void Start()
        {
            _lastPosition = _positionRepository.GetAsynch().Result;
            _local.SubscribeToAllFrom(_lastPosition, CatchUpSubscriptionSettings.Default, EventAppeared);
            _timer = new Timer(_interval);
            _timer.Elapsed += OnTimedEvent;
            _timer.Enabled = true;
            _timer.Start();
        }

        private async void OnTimedEvent(Object source, ElapsedEventArgs e)
        {
            await BulkIt();
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

            try
            {
                // We don't want to replicate an event back to its origin
                if (metadata.ContainsKey("$origin") && metadata["$origin"].Equals(_dispatcher.Destination))
                    return Task.CompletedTask;

                var metadataAsBytes = EnrichMetadata(resolvedEvent, metadata);

                _cache.Enqueue(new EventData(resolvedEvent.Event.EventId,
                    resolvedEvent.Event.EventType, resolvedEvent.Event.IsJson, resolvedEvent.Event.Data,
                    metadataAsBytes));
            }
            catch (Exception e)
            {
                Log.Error(e, "Error during geo-replication");
            }

            return Task.CompletedTask;
        }

        private async Task BulkIt()
        {
            try
            {
                var batch = new ArrayList();
                var count = _batchSize <= 0 ? _cache.Count : _cache.Count < _batchSize ? _cache.Count : _batchSize;
                for (var i = 0; i <= count; i++)
                {
                    if (_cache.TryDequeue(out var result))
                    {
                        batch.Add(result);
                    }
                }

                if (batch.Count == 0)
                    return;

                await _dispatcher.BulkAppendAsynch(_ingestionStreamName, batch.ToArray());
                Log.Information($"Dispatched {batch.Count} Events to {_dispatcher.Destination}/{_ingestionStreamName}");
            }
            catch (Exception e)
            {
                Log.Error(e, $"Log during bulk operation: {e.GetBaseException().Message}");
            }
        }

        private byte[] EnrichMetadata(ResolvedEvent resolvedEvent, IDictionary<string, string> metadata)
        {
            if (metadata.ContainsKey("$origin"))
                metadata["$origin"] = _dispatcher.Origin;
            else
                metadata.Add("$origin", _dispatcher.Origin);

            if (!metadata.ContainsKey("$applies"))
                metadata.Add("$applies", resolvedEvent.Event.Created.ToString("o"));

            if (!metadata.ContainsKey("$eventStreamId"))
                metadata.Add("$eventStreamId", resolvedEvent.Event.EventStreamId);

            //if (!metadata.ContainsKey("$position"))
            //    metadata.Add("$position", resolvedEvent.OriginalPosition.ToString());

            //if (!metadata.ContainsKey("$eventNumber"))
            //    metadata.Add("$eventNumber", resolvedEvent.Event.EventNumber.ToString());

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
