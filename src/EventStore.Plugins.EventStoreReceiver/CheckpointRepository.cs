using System;
using System.Collections.Generic;
using System.Linq;
using System.Text;
using System.Threading.Tasks;
using System.Timers;
using EventStore.ClientAPI;
using Newtonsoft.Json;

namespace EventStore.Plugins.EventStoreReceiver
{
    public class CheckpointRepository : ICheckpointRepository, IDisposable
    {
        private readonly string _streamName;
        public string EventType { get; }
        private readonly IEventStoreConnection _connection;
        private static Timer _timer;
        private long _lastCheckpoint;

        public CheckpointRepository(string streamName, string eventType, IEventStoreConnection connection, int interval)
        {
            _streamName = streamName;
            EventType = eventType;
            _connection = connection;
            InitStream();
            _timer = new Timer(interval);
            _timer.Elapsed += _timer_Elapsed;
            _timer.Enabled = true;
            _timer.Start();
        }

        private async void _timer_Elapsed(object sender, ElapsedEventArgs e)
        {
            if (_lastCheckpoint < 1)
                return;
            var evt = new EventData(Guid.NewGuid(), EventType, true, SerializeObject(new Checkpoint(_lastCheckpoint)), null);
            await _connection.AppendToStreamAsync(_streamName, ExpectedVersion.Any, evt);
        }

        private async Task InitStream()
        {
            var metadata = new Dictionary<string, int> { { "$maxCount", 1 } };
            await _connection.SetStreamMetadataAsync(_streamName, ExpectedVersion.Any, SerializeObject(metadata));
        }

        public async Task<long> GetAsynch()
        {
            var evts = await _connection.ReadStreamEventsBackwardAsync(_streamName, StreamPosition.End, 1, true);
            if (evts.Events.Any())
                return DeserializeObject<Checkpoint>(evts.Events[0].OriginalEvent.Data).Value;
            return StreamCheckpoint.StreamStart ?? 0;
        }

        public void Set(long checkpoint)
        {
            _lastCheckpoint = checkpoint;
        }

        private static byte[] SerializeObject(object obj)
        {
            var jsonObj = JsonConvert.SerializeObject(obj);
            var data = Encoding.UTF8.GetBytes(jsonObj);
            return data;
        }

        private static T DeserializeObject<T>(byte[] data)
        {
            return JsonConvert.DeserializeObject<T>(Encoding.ASCII.GetString(data));
        }

        public void Dispose()
        {
            _timer.Stop();
        }
    }

    public class Checkpoint
    {
        public long Value { get; }

        public Checkpoint(long checkpoint)
        {
            Value = checkpoint;
        }
    }
}
