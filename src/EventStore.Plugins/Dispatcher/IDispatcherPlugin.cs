namespace EventStore.Plugins.Dispatcher
{
    public interface IDispatcherPlugin : IEventStorePlugin
    {
        IDispatcherFactory GetStrategyFactory();
    }
}
