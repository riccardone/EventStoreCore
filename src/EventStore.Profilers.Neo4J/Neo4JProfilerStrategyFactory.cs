using EventStore.Core.Services.EventProfiler.Strategy;

namespace EventStore.Profilers.Neo4J
{
    public class Neo4JProfilerStrategyFactory : IEventProfilerStrategyFactory
    {
        public string StrategyName { get { return "Neo4JProfilerStrategyFactory"; } }
        public IEventProfilerStrategy Create()
        {
            return new Neo4JProfilerStrategy();
        }
    }
}
