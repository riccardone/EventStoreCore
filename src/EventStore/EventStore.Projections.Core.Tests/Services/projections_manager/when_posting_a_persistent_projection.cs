using System;
using System.Collections.Generic;
using System.Linq;
using System.Text;
using EventStore.Common.Utils;
using EventStore.Core.Bus;
using EventStore.Core.Messages;
using EventStore.Core.Messaging;
using EventStore.Projections.Core.Messages;
using EventStore.Projections.Core.Services;
using EventStore.Projections.Core.Services.Management;
using NUnit.Framework;

namespace EventStore.Projections.Core.Tests.Services.projections_manager
{
    [TestFixture]
    public class when_posting_a_persistent_projection : TestFixtureWithProjectionCoreAndManagementServices
    {
        private string _projectionName;

        protected override void Given()
        {
            _projectionName = "test-projection";
            AllWritesQueueUp();
        }

        protected override IEnumerable<WhenStep> When()
        {
            yield return new SystemMessage.BecomeMaster(Guid.NewGuid());
            AllWriteComplete();
            _consumer.HandledMessages.Clear();
            yield return
                new ProjectionManagementMessage.Command.Post(
                    new PublishEnvelope(_bus), ProjectionMode.Continuous, _projectionName,
                    ProjectionManagementMessage.RunAs.System, "JS", @"fromAll().whenAny(function(s,e){return s;});",
                    enabled: true, checkpointsEnabled: true, emitEnabled: true);
            OneWriteCompletes();
            yield return Yield;
        }

        [Test, Category("v8")]
        public void the_projection_status_is_writing()
        {
            _manager.Handle(
                new ProjectionManagementMessage.GetStatistics(new PublishEnvelope(_bus), null, _projectionName, true));
            Assert.AreEqual(
                ManagedProjectionState.Prepared,
                _consumer.HandledMessages.OfType<ProjectionManagementMessage.Statistics>().Single().Projections[0].
                    MasterStatus);
        }

        [Test, Category("v8")]
        public void a_projection_created_event_is_written()
        {
            Assert.AreEqual(
                "$ProjectionCreated",
                _consumer.HandledMessages.OfType<ClientMessage.WriteEvents>().First().Events[0].EventType);
            Assert.AreEqual(
                _projectionName,
                Helper.UTF8NoBom.GetString(_consumer.HandledMessages.OfType<ClientMessage.WriteEvents>().First().Events[0].Data));
        }

        [Test, Category("v8")]
        public void a_projection_updated_message_is_not_published()
        {
            // not published until all writes complete
            Assert.AreEqual(0, _consumer.HandledMessages.OfType<ProjectionManagementMessage.Updated>().Count());
        }

    }
}
