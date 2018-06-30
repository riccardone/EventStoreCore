using System;
using System.Collections;
using System.Collections.Concurrent;
using System.Collections.Generic;
using System.Linq;
using System.Text;
using System.Threading.Tasks;
using System.Timers;
using EventStore.ClientAPI;
using EventStore.Plugins.Dispatcher;
using Newtonsoft.Json;

namespace EventStore.Plugins.EventStoreDispatcher
{
    public class CachedEvent
    {
        public Position Position { get; }
        public EventData EventData { get; }

        public CachedEvent(Position position, EventData eventData)
        {
            Position = position;
            EventData = eventData;
        }
    }

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
        private readonly ConcurrentQueue<CachedEvent> _cache = new ConcurrentQueue<CachedEvent>();
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
                resolvedEvent.Event.EventType.Equals(_positionRepository.PositionEventType) || !resolvedEvent.OriginalPosition.HasValue)
                return Task.CompletedTask;

            var metadata = DeserializeObject(resolvedEvent.Event.Metadata) ?? new Dictionary<string, dynamic>();

            if (metadata.ContainsKey("$local"))
                return Task.CompletedTask;
            
            try
            {
                // We don't want to replicate an event back to any of its origins
                if (metadata.ContainsKey("$origin"))
                {
                    string[] origins = metadata["$origin"].Split(',');
                    if (origins.Any(origin => origin.Equals(_dispatcher.Destination)))
                        return Task.CompletedTask;
                } 

                var metadataAsBytes = EnrichMetadata(resolvedEvent, metadata);

                _cache.Enqueue(new CachedEvent(resolvedEvent.OriginalPosition.Value, new EventData(resolvedEvent.Event.EventId,
                    resolvedEvent.Event.EventType, resolvedEvent.Event.IsJson, resolvedEvent.Event.Data,
                    metadataAsBytes)));
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
                if (_cache.IsEmpty)
                    return;
                var batch = new List<CachedEvent>();
                var count = _batchSize <= 0 ? _cache.Count : _cache.Count < _batchSize ? _cache.Count : _batchSize;
                for (var i = 0; i <= count; i++)
                {
                    if (!_cache.TryDequeue(out var result))
                        continue;
                    batch.Add(result);
                }

                if (batch.Count == 0)
                    return;
                dynamic[] eventDatas = batch.Select(a => a.EventData).ToArray();
                await _dispatcher.AppendAsynch(_ingestionStreamName, eventDatas);
                var lastPosition = batch.Last().Position;
                await _positionRepository.SetAsynch(lastPosition);
                _lastPosition = lastPosition;
                Log.Information($"Dispatched {batch.Count} Events to {_dispatcher.Destination}/{_ingestionStreamName}");
            }
            catch (ObjectDisposedException)
            {
                Log.Error($"I can't connect to: '{_dispatcher.Destination}'");
            }
            catch (Exception e)
            {
                Log.Error($"Error from destination: '{_dispatcher.Destination}'");
                Log.Error(e.GetBaseException().Message);
            }
        }

        private byte[] EnrichMetadata(ResolvedEvent resolvedEvent, IDictionary<string, dynamic> metadata)
        {
            if (metadata.ContainsKey("$origin"))
                // This node is part of a replica chain and therefore we don't want to forget the previous origins
                metadata["$origin"] = $"{metadata["$origin"]},{_dispatcher.Origin}";
            else
                metadata.Add("$origin", _dispatcher.Origin);

            if (!metadata.ContainsKey("$applies"))
                metadata.Add("$applies", resolvedEvent.Event.Created.ToString("o"));

            if (!metadata.ContainsKey("$eventStreamId"))
                metadata.Add("$eventStreamId", resolvedEvent.Event.EventStreamId);

            if (!metadata.ContainsKey("$eventNumber"))
                metadata.Add("$eventNumber", resolvedEvent.Event.EventNumber);

            return SerializeObject(metadata);
        }

        private static IDictionary<string, dynamic> DeserializeObject(byte[] obj)
        {
            return JsonConvert.DeserializeObject<Dictionary<string, dynamic>>(
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
