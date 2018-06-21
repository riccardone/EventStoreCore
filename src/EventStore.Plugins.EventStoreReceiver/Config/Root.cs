namespace EventStore.Plugins.EventStoreReceiver.Config
{
    public class Root
    {
        public Root(Receiver origin)
        {
            Receiver = origin;
        }

        public Receiver Receiver { get; }
    }
}
