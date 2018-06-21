namespace EventStore.Plugins.Dispatcher
{
    public interface IDispatcherServiceFactory
    {
        IDispatcherService Create();
    }
}
