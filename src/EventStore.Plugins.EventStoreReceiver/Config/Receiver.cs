namespace EventStore.Plugins.EventStoreReceiver.Config
{
    public class Receiver
    {
        public Receiver(string name, string id, int localPort, string user, string password, string inputStream)
        {
            Name = name;
            Id = id;
            LocalPort = localPort;
            User = user;
            Password = password;
            InputStream = inputStream;
        }

        public string Name { get; }
        public string Id { get; }
        public int LocalPort { get; }
        public string User { get; }
        public string Password { get; }
        public string InputStream { get; }

        public override string ToString()
        {
            return $"{Name}-{Id}";
        }
    }
}
