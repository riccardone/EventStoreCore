using System;
using System.Collections.Generic;
using System.Text;
using System.Threading.Tasks;
using EventStore.ClientAPI;
using Newtonsoft.Json;

namespace EventStore.Plugins.EventStoreDispatcher
{
    public class PositionRepository : IPositionRepository
    {
        private readonly string _positionStreamName;
        private readonly string _positionStreamType;
        private readonly IEventStoreConnection _connection;

        public PositionRepository(string positionStreamName, string positionStreamType, IEventStoreConnection connection)
        {
            _positionStreamName = positionStreamName;
            _positionStreamType = positionStreamType;
            _connection = connection;
        }

        public Position Get()
        {
            var rawEvt = _connection.ReadEventAsync(_positionStreamName, StreamPosition.End, false).Result;
            if (rawEvt.Event != null)
            {
                var data = Encoding.ASCII.GetString(rawEvt.Event.Value.OriginalEvent.Data);
                // TODO return the position
            }
            return Position.Start;
        }

        public async Task SetAsynch(Position? position)
        {
            if (!position.HasValue)
                return;
            var pos = new Dictionary<string, string>
            {
                {"CommitPosition", position.Value.CommitPosition.ToString()},
                {"PreparePosition", position.Value.PreparePosition.ToString()}
            };
            var metadata = new Dictionary<string, string> { { "$maxCount", 1.ToString() } };
            var evt = new EventData(Guid.NewGuid(), _positionStreamType, true, SerializeObject(pos), SerializeObject(metadata));
            await _connection.AppendToStreamAsync(_positionStreamName, ExpectedVersion.Any, evt);
        }

        private static byte[] SerializeObject(object obj)
        {
            var jsonObj = JsonConvert.SerializeObject(obj);
            var data = Encoding.UTF8.GetBytes(jsonObj);
            return data;
        }
    }
}
