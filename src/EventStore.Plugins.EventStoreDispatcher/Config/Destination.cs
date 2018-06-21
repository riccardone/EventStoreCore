using System;

namespace EventStore.Plugins.EventStoreDispatcher.Config
{
    public class Destination
    {
        public Destination(string name, string id, Uri connectionString, string inputStream)
        {
            Name = name;
            Id = id;
            ConnectionString = connectionString;
            InputStream = inputStream;
        }

        public string Name { get; }
        public string Id { get; }
        public Uri ConnectionString { get; }
        public string InputStream { get; }

        public override string ToString()
        {
            return $"{Name}-{Id}";
        }
    }
}
