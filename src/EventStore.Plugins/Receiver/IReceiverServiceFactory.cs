namespace EventStore.Plugins.Receiver
{
    public interface IReceiverServiceFactory
    {
        string Name { get; }
        IReceiverService Create();
    }
}
