using EventStore.ClientAPI;
using EventStore.ClientAPI.SystemData;
using EventStore.Core.Bus;
using EventStore.Core.Data;
using EventStore.Core.Messages;
using EventStore.Core.Services;
using EventStore.Core.Tests.ClientAPI;
using EventStore.Core.Tests.ClientAPI.Helpers;
using NUnit.Framework;
using System;
using System.Collections.Generic;
using System.Linq;
using System.Text;

namespace EventStore.Core.Tests.AllReaderTests
{
    public class when_reading_all_with_mini_node : SpecificationWithMiniNode
    {
        private StorageMessage.CommitAck _lastCommitAck;
        private EventData[] _testEvents;
        protected override void When()
        {
            _node.Node.MainBus.Subscribe<StorageMessage.CommitAck>(new CommitAckHandler((msg) =>
            {
                _lastCommitAck = msg;
            }));
            _conn.SetStreamMetadataAsync("$all", -1,
                                    EventStore.ClientAPI.StreamMetadata.Build().SetReadRole(SystemRoles.All),
                                    new UserCredentials(SystemUsers.Admin, SystemUsers.DefaultAdminPassword))
            .Wait();

            _testEvents = Enumerable.Range(0, 1).Select(x => TestEvent.NewTestEvent(x.ToString())).ToArray();
            _conn.AppendToStreamAsync("stream", EventStore.ClientAPI.ExpectedVersion.EmptyStream, _testEvents).Wait();
        }

        [Test, Category("LongRunning")]
        public void expect_the_next_position_to_be_higher_than_last_committed_index_position()
        {
            var read = _conn.ReadAllEventsForwardAsync(Position.Start, int.MaxValue, false).Result;
            Assert.IsTrue(read.NextPosition.CommitPosition <= _lastCommitAck.LogPosition,
                 String.Format("The NextPosition's Commit Position of {0} is greater than the Last Commit's Log Position of {1}",
                            read.NextPosition.CommitPosition, _lastCommitAck.LogPosition));
        }
    }

    public class CommitAckHandler : IHandle<StorageMessage.CommitAck>
    {
        private Action<StorageMessage.CommitAck> _onCommitAck;
        public CommitAckHandler(Action<StorageMessage.CommitAck> onCommitAck)
        {
            _onCommitAck = onCommitAck;
        }
        public void Handle(StorageMessage.CommitAck message)
        {
            _onCommitAck(message);
        }
    }
}
