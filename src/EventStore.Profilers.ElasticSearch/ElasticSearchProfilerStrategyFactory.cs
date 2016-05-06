using EventStore.Core.Services.EventProfiler.Strategy;

namespace EventStore.Profilers.ElasticSearch
{
    public class ElasticSearchProfilerStrategyFactory : IEventProfilerStrategyFactory
    {
        public string StrategyName { get { return "ElasticSearchProfilerStrategyFactory"; } }
        public IEventProfilerStrategy Create()
        {
            return new ElasticSearchProfilerStrategy();
        }
    }
}
