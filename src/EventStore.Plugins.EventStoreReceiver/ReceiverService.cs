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

        public ReceiverService(Root settings, IEventStoreConnection local)
        {
            _settings = settings;
            _local = local;
        }

        public void Start()
        {
            _local.SubscribeToStreamFrom(_settings.Receiver.InputStream, 0, CatchUpSubscriptionSettings.Default, EventAppeared);
        }

        private async Task EventAppeared(EventStoreCatchUpSubscription eventStoreCatchUpSubscription, ResolvedEvent resolvedEvent)
        {
            // Allow only user events and metadata events
            if (resolvedEvent.Event.EventType.StartsWith("$") && !resolvedEvent.Event.EventType.StartsWith("$$$"))
                return;

            var metadata = DeserializeObject(resolvedEvent.Event.Metadata);

            try
            {
                await _local.AppendToStreamAsync(metadata["$eventStreamId"], int.Parse(metadata["$expectedVersion"]),
                    new EventData(resolvedEvent.Event.EventId, resolvedEvent.Event.EventType, resolvedEvent.Event.IsJson,
                        resolvedEvent.Event.Data, resolvedEvent.Event.Metadata));
            }
            catch (WrongExpectedVersionException ex)
            {
                await HandleConflict(resolvedEvent, ex, metadata, SerializeObject(metadata));
                Log.Warning("WrongExpectedVersionException thrown during ingestion of replicated events: {0}", ex);
            }
            catch (Exception e)
            {
                Log.Error(e, "Error during geo-replication ingestion");
            }
        }

        private async Task HandleConflict(ResolvedEvent resolvedEvent, WrongExpectedVersionException ex, IDictionary<string, string> metadata, byte[] metadataAsBytes)
        {
            var conflictStreamName = $"$conflicts-{metadata["origin"]}-{_settings.Receiver}";
            await _local.AppendToStreamAsync(conflictStreamName, ExpectedVersion.Any,
                new EventData(resolvedEvent.Event.EventId, resolvedEvent.Event.EventType, resolvedEvent.Event.IsJson,
                    resolvedEvent.Event.Data, metadataAsBytes));
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
