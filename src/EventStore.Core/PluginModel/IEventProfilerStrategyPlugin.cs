using EventStore.Core.Services.EventProfiler.Strategy;

namespace EventStore.Core.PluginModel
{
    public interface IEventProfilerStrategyPlugin
    {
        string Name { get; }

        string Version { get; }

        IEventProfilerStrategyFactory GetStrategyFactory();
    }
}
