using System;
using System.Collections.Generic;
using EventStore.Common.Log;
using EventStore.Core.Bus;
using EventStore.Core.Data;
using EventStore.Core.Messages;
using EventStore.Core.Services.EventProfiler.Strategy;

namespace EventStore.Core.Services.EventProfiler
{
    public class EventProfilerService : 
        IHandle<SystemMessage.BecomeMaster>, 
        IHandle<StorageMessage.EventCommitted>,
        IHandle<SystemMessage.StateChangeMessage>
    {
        private bool _started = false;
        private static readonly ILogger Log = LogManager.GetLoggerFor<EventProfilerService>();
        private Dictionary<string, IEventProfilerStrategy> _profilers;
        private VNodeState _state;
        private readonly IEventProfilerStrategyFactory[] _profilerFactories;

        internal EventProfilerService(IEventProfilerStrategyFactory[] profilerFactories)
        {
            _profilerFactories = profilerFactories;
        }

        public void Handle(StorageMessage.EventCommitted message)
        {
            if (!_started || message.Event.EventStreamId.StartsWith("$")) return;
            ProcessEventCommited(message.Event.EventStreamId, message.CommitPosition, message.Event);
        }

        private void Start()
        {
            foreach (var eventProfilerStrategyFactory in _profilerFactories)
                _profilers.Add(eventProfilerStrategyFactory.StrategyName, eventProfilerStrategyFactory.Create());
            _started = true;
        }

        private void Stop()
        {
            _started = false;
        }

        private void ProcessEventCommited(string eventStreamId, long commitPosition, EventRecord evnt)
        {
            var pair = ResolvedEvent.ForUnresolvedEvent(evnt, commitPosition);
            foreach (var eventProfilerStrategy in _profilers)
            {
                eventProfilerStrategy.Value.PushMessageToProfiler(eventStreamId, pair);
            }
        }

        public void Handle(SystemMessage.BecomeMaster message)
        {
            Log.Debug("EventProfiler Became Master so now handling committed messages to be sent to profilers");
            InitToEmpty();
            LoadConfiguration(Start);
        }

        private void InitToEmpty()
        {
            _profilers = new Dictionary<string, IEventProfilerStrategy>();
        }

        private void LoadConfiguration(Action continueWith)
        {
            //_ioDispatcher.ReadBackward(SystemStreams.PersistentSubscriptionConfig, -1, 1, false,
            //    SystemAccount.Principal, x => HandleLoadCompleted(continueWith, x));

            // TODO

            continueWith();
        }

        public void Handle(SystemMessage.StateChangeMessage message)
        {
            _state = message.State;

            if (message.State == VNodeState.Master) return;
            Log.Debug(string.Format("EventProfiler received state change to {0} stopping listening.", _state));
            ShutdownProfilers();
            Stop();
        }

        private void ShutdownProfilers()
        {
            // TODO
        }
    }
}
