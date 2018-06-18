using System;

namespace EventStore.Plugins.EventStoreDispatcher.Config
{
    public class Origin
    {
        public Origin(string name, string id, int localPort, string user, string password)
        {
            Name = name;
            Id = id;
            LocalPort = localPort;
            User = user;
            Password = password;
        }

        public string Name { get; }
        public string Id { get; }
        public int LocalPort { get; }
        public string User { get; }
        public string Password { get; }

        public override string ToString()
        {
            return $"{Name}-{Id}";
        }
    }
}
