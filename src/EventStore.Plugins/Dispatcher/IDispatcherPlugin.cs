namespace EventStore.Plugins.Dispatcher
{
    public interface IDispatcherPlugin : IEventStorePlugin
    {
        ISubscriberServiceFactory GetStrategyFactory();
    }
}
