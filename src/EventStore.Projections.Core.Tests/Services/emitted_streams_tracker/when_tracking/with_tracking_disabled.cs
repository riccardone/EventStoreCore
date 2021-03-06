using EventStore.ClientAPI.SystemData;
using EventStore.Projections.Core.Services.Processing;
using NUnit.Framework;
using System;
using System.Threading;
using System.Threading.Tasks;

namespace EventStore.Projections.Core.Tests.Services.emitted_streams_tracker.when_tracking {
	public class with_tracking_disabled : SpecificationWithEmittedStreamsTrackerAndDeleter {
		private CountdownEvent _eventAppeared = new CountdownEvent(1);
		private UserCredentials _credentials = new UserCredentials("admin", "changeit");

		protected override void Given() {
			_trackEmittedStreams = false;
			base.Given();
		}

		protected override void When() {
			var sub = _conn.SubscribeToStreamAsync(_projectionNamesBuilder.GetEmittedStreamsName(), true, (s, evnt) => {
				_eventAppeared.Signal();
				return Task.CompletedTask;
			}, userCredentials: _credentials).Result;

			_emittedStreamsTracker.TrackEmittedStream(new EmittedEvent[] {
				new EmittedDataEvent(
					"test_stream", Guid.NewGuid(), "type1", true,
					"data", null, CheckpointTag.FromPosition(0, 100, 50), null, null)
			});

			_eventAppeared.Wait(TimeSpan.FromSeconds(5));
			sub.Unsubscribe();
		}

		[Test]
		public void should_write_a_stream_tracked_event() {
			var result = _conn.ReadStreamEventsForwardAsync(_projectionNamesBuilder.GetEmittedStreamsName(), 0, 200,
				false, _credentials).Result;
			Assert.AreEqual(0, result.Events.Length);
			Assert.AreEqual(1, _eventAppeared.CurrentCount); //no event appeared should get through
		}
	}
}
