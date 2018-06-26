using System;

namespace EventStore.Plugins.EventStoreDispatcher.Config
{
    public class Destination
    {
        public Destination(string name, string id, Uri connectionString, string inputStream, int interval, int batchSize)
        {
            Name = name;
            Id = id;
            ConnectionString = connectionString;
            InputStream = inputStream;
            Interval = interval;
            BatchSize = batchSize;
        }

        public string Name { get; }
        public string Id { get; }
        public Uri ConnectionString { get; }
        public string InputStream { get; }
        public int Interval { get; }
        public int BatchSize { get; }

        public override string ToString()
        {
            return $"{Name}-{Id}";
        }
    }
}
