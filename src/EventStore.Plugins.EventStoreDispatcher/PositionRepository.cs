using System;
using System.Collections.Generic;
using System.Linq;
using System.Text;
using System.Threading.Tasks;
using EventStore.ClientAPI;
using Newtonsoft.Json;

namespace EventStore.Plugins.EventStoreDispatcher
{
    public class PositionRepository : IPositionRepository
    {
        private readonly string _positionStreamName;
        public string PositionEventType { get; }
        private readonly IEventStoreConnection _connection;

        public PositionRepository(string positionStreamName, string positionEventType, IEventStoreConnection connection)
        {
            _positionStreamName = positionStreamName;
            PositionEventType = positionEventType;
            _connection = connection;
            InitStream();
        }

        private async Task InitStream()
        {
            var metadata = new Dictionary<string, int> { { "$maxCount", 1 } };
            await _connection.SetStreamMetadataAsync(_positionStreamName, ExpectedVersion.Any, SerializeObject(metadata));
        }

        public async Task<Position> GetAsynch()
        {
            var evts = await _connection.ReadStreamEventsBackwardAsync(_positionStreamName, StreamPosition.End, 1, true);
            if (evts.Events.Any())
                return DeserializeObject<Position>(evts.Events[0].OriginalEvent.Data);
            return Position.Start;
        }

        public async Task SetAsynch(Position position)
        {
            var evt = new EventData(Guid.NewGuid(), PositionEventType, true, SerializeObject(position), null);
            await _connection.AppendToStreamAsync(_positionStreamName, ExpectedVersion.Any, evt);
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
    }
}
