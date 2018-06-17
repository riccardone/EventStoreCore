using System;

namespace EventStore.Plugins.EventStoreDispatcher.Config
{
    public class Destination
    {
        public Destination(string name, string id, Uri connectionString)
        {
            Name = name;
            Id = id;
            ConnectionString = connectionString;
        }

        public string Name { get; }
        public string Id { get; }
        public Uri ConnectionString { get; }

        public override string ToString()
        {
            return $"{Name}-{Id}";
        }
    }
}
