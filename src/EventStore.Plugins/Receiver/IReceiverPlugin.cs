namespace EventStore.Plugins.Receiver
{
    public interface IReceiverPlugin : IEventStorePlugin
    {
        IReceiverServiceFactory GetStrategyFactory();
    }
}
