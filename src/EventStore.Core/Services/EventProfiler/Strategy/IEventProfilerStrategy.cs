using EventStore.Core.Data;

namespace EventStore.Core.Services.EventProfiler.Strategy
{
    public interface IEventProfilerStrategy
    {
        string Name { get; }

        // TODO do it asynch
        EventProfilerPushResult PushMessageToProfiler(string streamId, ResolvedEvent ev);
    }

    public enum EventProfilerPushResult // TODO do we need this?
    {
        Sent,
        Skipped, // TODO do we need this?
        NoMoreCapacity // TODO do we need this?
    }
}
