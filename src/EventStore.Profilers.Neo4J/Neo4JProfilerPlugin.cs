using System.ComponentModel.Composition;
using EventStore.Core.PluginModel;
using EventStore.Core.Services.EventProfiler.Strategy;

namespace EventStore.Profilers.Neo4J
{
    [Export(typeof(IEventProfilerStrategyPlugin))]
    public class Neo4JProfilerPlugin : IEventProfilerStrategyPlugin
    {
        public string Name { get { return "Neo4JProfilerPlugin"; } }
        public string Version { get { return "1.0"; } }
        public IEventProfilerStrategyFactory GetStrategyFactory()
        {
            return new Neo4JProfilerStrategyFactory();
        }
    }
}
