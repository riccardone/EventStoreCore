namespace EventStore.Plugins.EventStoreReceiver.Config
{
    public class Local
    {
        public Local(string name, int port)
        {
            Name = name;
            Port = port;
        }

        public string Name { get; }
        public int Port { get; }

        public override string ToString()
        {
            return Name;
        }
    }
}
