using EventStore.Core.Bus;

namespace EventStore.Core.Services.EventProfiler.Strategy
{
    public interface IEventProfilerStrategyFactory
    {
        string StrategyName { get; }

        IEventProfilerStrategy Create();
    }
}
