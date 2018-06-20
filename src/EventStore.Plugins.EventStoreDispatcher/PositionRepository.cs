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
        }

        public async Task<Position> GetAsynch()
        {
            return Position.Start;
            // TODO implement this if you want to get last position
            //var evts = await _connection.ReadStreamEventsBackwardAsync(_positionStreamName, StreamPosition.End, 1, true);
            //if (evts.Events.Any())
            //{
            //    var data = Encoding.ASCII.GetString(evts.Events[0].OriginalEvent.Data);
            //    // TODO return the position
            //}
            //return Position.Start;
        }

        public async Task SetAsynch(Position? position)
        {
            if (!position.HasValue)
                return;
            return;
            // TODO implement this if you want to save the position
            //var pos = new Dictionary<string, string>
            //{
            //    {"CommitPosition", position.Value.CommitPosition.ToString()},
            //    {"PreparePosition", position.Value.PreparePosition.ToString()}
            //};
            //var metadata = new Dictionary<string, string> { { "$maxCount", 1.ToString() } };
            //var evt = new EventData(Guid.NewGuid(), PositionEventType, true, SerializeObject(pos), SerializeObject(metadata));
            //await _connection.AppendToStreamAsync(_positionStreamName, ExpectedVersion.Any, evt);
        }

        private static byte[] SerializeObject(object obj)
        {
            var jsonObj = JsonConvert.SerializeObject(obj);
            var data = Encoding.UTF8.GetBytes(jsonObj);
            return data;
        }
    }
}
