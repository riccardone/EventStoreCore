namespace EventStore.Core.PluginModel
{
    public interface IEventAnalyser
    {
        void Save(dynamic evt);
    }
}
