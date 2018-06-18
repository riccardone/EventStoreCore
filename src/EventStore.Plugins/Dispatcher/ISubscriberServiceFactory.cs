namespace EventStore.Plugins.Dispatcher
{
    public interface ISubscriberServiceFactory
    {
        ISubscriberService Create();
    }
}
