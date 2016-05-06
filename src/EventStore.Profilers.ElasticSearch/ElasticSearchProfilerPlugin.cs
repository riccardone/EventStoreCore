using System.ComponentModel.Composition;
using EventStore.Core.PluginModel;
using EventStore.Core.Services.EventProfiler.Strategy;

namespace EventStore.Profilers.ElasticSearch
{
    [Export(typeof(IEventProfilerStrategyPlugin))]
    public class ElasticSearchProfilerPlugin : IEventProfilerStrategyPlugin
    {
        public string Name { get { return "ElasticSearchProfilerPlugin"; } }
        public string Version { get { return "1.0"; } }
        public IEventProfilerStrategyFactory GetStrategyFactory()
        {
            return new ElasticSearchProfilerStrategyFactory();
        }
    }
}
