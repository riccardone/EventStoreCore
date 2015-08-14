using System;
using System.Threading;
using EventStore.Core.Bus;
using EventStore.Core.Data;
using EventStore.Core.Messages;
using EventStore.Core.Messaging;
using EventStore.Core.Services.UserManagement;
using NUnit.Framework;
using System.Linq;

namespace EventStore.Core.Tests.AllReaderTests
{
    [TestFixture, Category("AllReaderScenario")]
    internal class when_reading_all
    {
        private SimulatedStorageService _simulatedStorageService;
        private ManualResetEventSlim _writeCompletedEvent;
        private ManualResetEventSlim _readCompletedEvent;
        private InternalReplyEnvelope _readEventsEnvelope;
        private StorageMessage.CommitAck _lastCommitAck;
        public const string StreamId = "teststream";
        [TestFixtureSetUp]
        public void TestFixtureSetUp()
        {
            _writeCompletedEvent = new ManualResetEventSlim(false);
            _simulatedStorageService = new SimulatedStorageService();

            new TestHandlers(_simulatedStorageService.Bus, () =>
            {
                //ClusterVNodeController  Forwards - Queue => Bus
                _simulatedStorageService.Bus.Publish(new ClientMessage.WriteEvents(Guid.NewGuid(), Guid.NewGuid(),
                    new InternalReplyEnvelope(_writeCompletedEvent), false, StreamId, ExpectedVersion.Any,
                    new Event(Guid.NewGuid(), "eventType", true, "{\"key\":\"value\"}", null), SystemAccount.Principal));
            }, (commitAck) =>
            {
                _lastCommitAck = commitAck;
            });

            _readCompletedEvent = new ManualResetEventSlim(false);
            _readEventsEnvelope = new InternalReplyEnvelope(_readCompletedEvent);

            _simulatedStorageService.Start();
            _writeCompletedEvent.Wait();
        }

        [TestFixtureTearDown]
        public void TestFixtureTearDown()
        {
            _simulatedStorageService.Stop();
        }

        [Test]
        public void expect_the_next_position_to_be_higher_than_last_committed_index_position()
        {
            _simulatedStorageService.Bus.Publish(CreateReadAllEventsForward(_readEventsEnvelope));
            _readCompletedEvent.Wait();
            ClientMessage.ReadAllEventsForwardCompleted msg = (ClientMessage.ReadAllEventsForwardCompleted)_readEventsEnvelope.Replies[0];
            Assert.IsTrue(msg.Result == ReadAllResult.Success);
            Assert.IsTrue(msg.NextPos.CommitPosition <= _lastCommitAck.LogPosition,
                 String.Format("The NextPosition's Commit Position of {0} is greater than the Last Commit's Log Position of {1}",
                            msg.NextPos.CommitPosition, _lastCommitAck.LogPosition));
        }

        public ClientMessage.ReadAllEventsForward CreateReadAllEventsForward(IEnvelope envelope)
        {
            return new ClientMessage.ReadAllEventsForward(Guid.NewGuid(), Guid.NewGuid(), envelope, 0, 0, int.MaxValue, true, true, 0, SystemAccount.Principal);
        }
    }

    internal class TestHandlers :
        IHandle<SystemMessage.SystemStart>,
        IHandle<StorageMessage.CommitAck>
    {
        private readonly IBus _outputBus;
        private readonly Action _onStart;
        private readonly Action<StorageMessage.CommitAck> _onIndexCommitted;

        public TestHandlers(IBus outputBus, Action onStart, Action<StorageMessage.CommitAck> onIndexCommitted)
        {
            _outputBus = outputBus;
            _onStart = onStart;
            _onIndexCommitted = onIndexCommitted;

            _outputBus.Subscribe<SystemMessage.SystemStart>(this);
            _outputBus.Subscribe<StorageMessage.CommitAck>(this);
        }

        public void Handle(SystemMessage.SystemStart message)
        {
            _onStart();
        }
        public void Handle(StorageMessage.CommitAck message)
        {
            _onIndexCommitted(message);
        }
    }
}
