using System.Threading.Tasks;
using EventStore.Core.Data;

namespace EventStore.Core.Services.EventProfiler.Strategy
{
    public interface IEventProfilerStrategy
    {
        string Name { get; }
        void PushMessageToProfiler(string streamId, ResolvedEvent ev);
    }
}
