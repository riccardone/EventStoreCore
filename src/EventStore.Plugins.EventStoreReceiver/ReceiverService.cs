using System;
using System.Collections.Generic;
using System.Text;
using System.Threading.Tasks;
using EventStore.ClientAPI;
using EventStore.ClientAPI.Exceptions;
using EventStore.Plugins.EventStoreReceiver.Config;
using EventStore.Plugins.Receiver;
using Newtonsoft.Json;

namespace EventStore.Plugins.EventStoreReceiver
{
    public class ReceiverService : IReceiverService
    {
        private static readonly Serilog.ILogger Log = Serilog.Log.ForContext<ReceiverService>();
        private readonly Root _settings;
        private readonly IEventStoreConnection _local;
        private long? _lastCheckpoint;

        public ReceiverService(Root settings, IEventStoreConnection local)
        {
            _settings = settings;
            _local = local;
        }

        public void Start()
        {
            Subscribe(_lastCheckpoint ?? StreamCheckpoint.StreamStart);
        }

        private void Subscribe(long? checkPoint)
        {
            _local.SubscribeToStreamFrom(_settings.Receiver.InputStream, checkPoint, CatchUpSubscriptionSettings.Default, EventAppeared, LiveProcessingStarted, SubscriptionDropped);
        }

        private void SubscriptionDropped(EventStoreCatchUpSubscription arg1, SubscriptionDropReason arg2, Exception arg3)
        {
            Log.Warning(arg3, $"ReceiverService SubscriptionDropped {arg3.GetBaseException().Message}");
            Log.Warning("ReceiverService Resubscribing...");
            Subscribe(_lastCheckpoint ?? StreamCheckpoint.StreamStart);
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
                await _local.AppendToStreamAsync(metadata["$eventStreamId"], int.Parse(metadata["$expectedVersion"]),
                    new EventData(resolvedEvent.Event.EventId, resolvedEvent.Event.EventType, resolvedEvent.Event.IsJson,
                        resolvedEvent.Event.Data, resolvedEvent.Event.Metadata));
                _lastCheckpoint = resolvedEvent.OriginalEventNumber;
            }
            catch (WrongExpectedVersionException ex)
            {
                Log.Warning("WrongExpectedVersionException thrown during ingestion of replicated events: {0}", ex);
                metadata.Add("$error", ex.GetBaseException().Message);
                if (_settings.Receiver.AppendInCaseOfConflict)
                {
                    await _local.AppendToStreamAsync(metadata["$eventStreamId"], ExpectedVersion.Any,
                        new EventData(resolvedEvent.Event.EventId, resolvedEvent.Event.EventType, resolvedEvent.Event.IsJson,
                            resolvedEvent.Event.Data, SerializeObject(metadata)));
                }
                else
                {
                    var conflictStreamName = $"$conflicts-{metadata["$origin"]}-{_settings.Receiver}";
                    await HandleConflict(conflictStreamName, resolvedEvent, ex, metadata, SerializeObject(metadata));
                    Log.Warning($"Published conflict to '{conflictStreamName}' stream");
                }
            }
            catch (Exception e)
            {
                Log.Error(e, "Error during geo-replication ingestion");
            }
        }

        private async Task HandleConflict(string conflictStream, ResolvedEvent resolvedEvent, WrongExpectedVersionException ex, IDictionary<string, string> metadata, byte[] metadataAsBytes)
        {
            try
            {
                await _local.AppendToStreamAsync(conflictStream, ExpectedVersion.Any,
                    new EventData(resolvedEvent.Event.EventId, resolvedEvent.Event.EventType, resolvedEvent.Event.IsJson,
                        resolvedEvent.Event.Data, metadataAsBytes));
            }
            catch (Exception e)
            {
                Log.Error(e, "HandleConflict error");
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
