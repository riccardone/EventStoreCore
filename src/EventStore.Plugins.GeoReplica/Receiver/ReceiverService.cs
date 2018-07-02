using System;
using System.Collections.Generic;
using System.Text;
using System.Threading.Tasks;
using EventStore.ClientAPI;
using EventStore.ClientAPI.Exceptions;
using Newtonsoft.Json;

namespace EventStore.Plugins.GeoReplica.Receiver
{
    public class ReceiverService : IEventStoreService
    {
        private static readonly Serilog.ILogger Log = Serilog.Log.ForContext<ReceiverService>();
        private readonly Config.Receiver _receiver;
        private readonly IEventStoreConnection _local;
        private readonly ICheckpointRepository _checkpointRepository;

        public ReceiverService(Config.Receiver receiver, IEventStoreConnection local, ICheckpointRepository checkpointRepository)
        {
            _receiver = receiver;
            _local = local;
            _checkpointRepository = checkpointRepository;
        }

        public void Start()
        {
            Subscribe(_checkpointRepository.GetAsynch().Result);
        }

        private void Subscribe(long? checkPoint)
        {
            long? lastCheckPoint = null;
            if (checkPoint.HasValue)
                lastCheckPoint = checkPoint;
            _local.SubscribeToStreamFrom(_receiver.InputStream, lastCheckPoint ?? StreamCheckpoint.StreamStart,
                CatchUpSubscriptionSettings.Default, EventAppeared, LiveProcessingStarted, SubscriptionDropped);
        }

        private void SubscriptionDropped(EventStoreCatchUpSubscription arg1, SubscriptionDropReason arg2, Exception arg3)
        {
            Log.Warning($"ReceiverService SubscriptionDropped. SubscriptionDropReason: '{arg2.ToString()}'");
            if (arg3 != null)
                Log.Warning(arg3, $"Error: {arg3.GetBaseException().Message}");
            Log.Warning("ReceiverService Resubscribing...");
            Subscribe(_checkpointRepository.GetAsynch().Result);
        }

        private void LiveProcessingStarted(EventStoreCatchUpSubscription obj)
        {
            Log.Information("ReceiverService LiveProcessingStarted");
        }

        private async Task EventAppeared(EventStoreCatchUpSubscription eventStoreCatchUpSubscription, ResolvedEvent resolvedEvent)
        {
            // Allow only user events and metadata events
            if (resolvedEvent.Event.EventType.StartsWith("$"))
                return;
            
            var metadata = DeserializeObject(resolvedEvent.Event.Metadata);

            try
            {
                // TODO review why the Expected Version error. It's wrong most of the time (it's 1 more than the current)
                long eventNumber = long.Parse(metadata["$eventNumber"]);
                if (eventNumber == 0)
                    eventNumber = -1;

                await _local.AppendToStreamAsync(metadata["$eventStreamId"], eventNumber,
                    new EventData(resolvedEvent.Event.EventId, resolvedEvent.Event.EventType,
                        resolvedEvent.Event.IsJson, resolvedEvent.Event.Data, resolvedEvent.Event.Metadata));
                _checkpointRepository.Set(resolvedEvent.OriginalEventNumber);
            }
            catch (WrongExpectedVersionException ex)
            {
                await HandleConflict(resolvedEvent, ex, metadata);
            }
            catch (Exception e)
            {
                Log.Error(e, "Error during geo-replication ingestion");
            }
        }

        private async Task HandleConflict(ResolvedEvent resolvedEvent, WrongExpectedVersionException ex, IDictionary<string, dynamic> metadata)
        {
            try
            {
                if (metadata.ContainsKey("$errors"))
                    metadata["$errors"] = $"{metadata["$errors"]},{_receiver}:{ex.GetBaseException().Message}";
                else
                    metadata.Add("$errors", $"{_receiver}:{ex.GetBaseException().Message}");
                if (_receiver.AppendInCaseOfConflict)
                {
                    await _local.AppendToStreamAsync(metadata["$eventStreamId"], ExpectedVersion.Any,
                        new EventData(resolvedEvent.Event.EventId, resolvedEvent.Event.EventType, resolvedEvent.Event.IsJson,
                            resolvedEvent.Event.Data, SerializeObject(metadata)));
                }
                else
                {
                    Log.Warning($"WrongExpectedVersionException while ingesting replicated events");
                    Log.Warning(ex.GetBaseException().Message);
                    var conflictStreamName = $"$conflicts-{metadata["$origin"]}-{_receiver}";
                    await _local.AppendToStreamAsync(conflictStreamName, ExpectedVersion.Any,
                        new EventData(resolvedEvent.Event.EventId, resolvedEvent.Event.EventType, resolvedEvent.Event.IsJson,
                            resolvedEvent.Event.Data, SerializeObject(metadata)));
                    Log.Warning($"Published conflict to '{conflictStreamName}' stream");
                }
            }
            catch (Exception e)
            {
                Log.Error(e, "Error during HandleConflict");
            }
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
