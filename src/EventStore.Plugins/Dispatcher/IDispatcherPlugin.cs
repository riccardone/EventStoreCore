namespace EventStore.Plugins.Dispatcher
{
    public interface IDispatcherPlugin : IEventStorePlugin
    {
        IDispatcherServiceFactory GetStrategyFactory();
    }
}
